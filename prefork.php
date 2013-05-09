<?php

class Prefork {
	// Resource limits for multi-intern workers to observe
	public $min_free_ram = 0.20; // Minimum free RAM (MemFree / MemTotal)

	// Service config
	public $heartbeat_bpm = 100; // Heartbeats per minute
	public $heartbeat_callback;  // Optional: callback to run on heartbeat
	public $max_workers = 1;     // How many workers should load the app
	public $single_interns = true; // Limit interns to one per worker
	public $prefork_callback;    // Optional: to be run before forking
	public $postfork_callback;   // Optional: to be run after forking

	// Sockets config -- use different ports if you run multiple services
	public $request_address = "127.0.0.1";
	public $request_port = 8300;
	public $request_backlog = 64;
	public $offer_address = "127.0.0.1";
	public $offer_port = 8310;
	public $offer_backlog = 64;
	public $response_address = "127.0.0.1";
	public $response_port = 8320;
	public $response_backlog = 64;

	// Sockets for listening
	private $request_socket;  // for Agents transacting with Service 
	private $offer_socket;    // for Workers offering to handle Requests
	private $response_socket; // for Interns/Workers returning Responses

	// Service state
	private $is_service;
	private $event_base;
	private $received_SIGINT; // true: reminder to send self SIGINT on exit
	private $service_shutdown; // true: service is shutting down
	private $workers_alive = array();      // worker_pid => time
	private $workers_starting = array();   // worker_pid => time
	private $workers_ready = array();      // worker_pid => offer_id
	private $workers_assigned = array();   // worker_pid => request_id
	private $workers_obsolete = array();   // worker_pid => time
	private $offers_sockets = array();     // offer_id => socket
	private $offers_buffering = array();   // offer_id => event buffer
	private $offers_buffered = array();    // offer_id => event buffer
	private $offers_written = array();     // offer_id => event buffer
	private $offers_requests = array();    // offer_id => request_id
	private $offers_workers = array();     // offer_id => worker_pid
	private $requests_accepted = array();  // request_id => time
	private $requests_sockets = array();   // request_id => socket
	private $requests_lengths = array();   // request_id => int
	private $requests_buffering = array(); // request_id => event buffer
	private $requests_buffered = array();  // request_id => event buffer
	private $requests_written = array();   // request_id => event buffer
	private $requests_working = array();   // request_id => event buffer

	// Worker/intern state
	private $is_worker;
	private $is_intern;
	private $worker_socket;
	private $request_id;

	public function become_agent() {
		$request = $this->agent__package_request();
		$response = $this->agent__transact_with_service( $request );
		// FALSE means the request was not processed at all
		if ( $response === false )
			return false;
		$this->agent__output_response( $response );
		return true;
	}

	public function become_service() {
		if ( version_compare( PHP_VERSION, '5.4', '<' ) )
			die( 'Error: Prefork requires PHP 5.4 for http_response_code().' . PHP_EOL );
		if ( ! defined( 'STDERR' ) )
			define( 'STDERR', fopen( 'php://stderr', 'w' ) );
		if ( ! $this->service__create_sockets() )
			return false;
		$this->is_service = true;
		// The service stays in this call while workers return from it
		$this->service__event_loop();
		return true;
	}

	private function service__event_loop() {
		// Set up the event loop
		$this->event_base = event_base_new();
		// Signal handlers
		$this->event_add( 'SIGCHLD', SIGCHLD, EV_SIGNAL | EV_PERSIST,
			'service__SIGCHLD' );
		$this->event_add( 'SIGHUP', SIGHUP, EV_SIGNAL | EV_PERSIST,
			'service__SIGHUP' );
		$this->event_add( 'SIGINT', SIGINT, EV_SIGNAL | EV_PERSIST,
			'service__SIGINT' );
		// Socket handlers
		$this->event_add( 'request', $this->request_socket,
			EV_READ | EV_PERSIST, 'service__accept_request' );
		$this->event_add( 'offer', $this->offer_socket,
			EV_READ | EV_PERSIST, 'service__accept_offer' );
		$this->event_add( 'response', $this->response_socket,
			EV_READ | EV_PERSIST, 'service__accept_response' );
		// Heartbeat
		$timeout_microseconds = intval( 60e6 / $this->heartbeat_bpm );
		$this->event_add( 'heartbeat', 0, EV_TIMEOUT | EV_PERSIST,
			'service__heartbeat', $timeout_microseconds );
		// Initial worker start (run only once)
		$this->event_add( 'start', 0, EV_TIMEOUT,
			'service__supervise_workers', 1 );
		event_base_loop( $this->event_base );
		if ( ! $this->is_worker ) {
			// Only workers should reach this code.
			fwrite( STDERR, "Service left event loop" . PHP_EOL );
			exit(1);
		}
	}

	public function service__heartbeat() {
		if ( is_callable( $this->heartbeat_callback ) )
			call_user_func( $this->heartbeat_callback );
		if ( $this->service_shutdown )
			$this->service__continue_shutdown();
		$this->service__supervise_workers();
	}

	public function service__accept_request() {
		$start_time = microtime( true );
		$socket = socket_accept( $this->request_socket );
		$request_id = (string) intval( $socket );
		$this->requests_sockets[ $request_id ] = $socket;
		$this->requests_accepted[ $request_id ] = $start_time;
		// Buffer the 4-byte header
		$event = event_buffer_new( $socket,
			array( $this, 'service__read_request_header' ),
			null,
			array( $this, 'service__read_request_header_error' )
		);
		event_buffer_timeout_set( $event, 1, 1 );
		event_buffer_watermark_set( $event, EV_READ, 4, 4 );
		event_buffer_base_set( $event, $this->event_base );
		event_buffer_enable( $event, EV_READ );
		$this->requests_buffering[ $request_id ] = $event;
	}

	public function service__read_request_header_error( $event, $what ) {
		$request_id = array_search( $event, $this->requests_buffering, true );
		$socket = $this->requests_sockets[ $request_id ];
		event_buffer_disable( $event, EV_READ | EV_WRITE );
		event_buffer_free( $event );
		socket_shutdown( $socket );
		socket_close( $socket );
		unset( $this->requests_accepted[ $request_id ] );
		unset( $this->requests_sockets[ $request_id ] );
		unset( $this->requests_buffering[ $request_id ] );
	}

	public function service__read_request_header( $event ) {
		$request_id = array_search( $event, $this->requests_buffering, true );
		$header = event_buffer_read( $event, 4 );
		$length = current( unpack( 'N', $header ) );
		$this->requests_lengths[ $request_id ] = $length;
		event_buffer_watermark_set( $event, EV_READ, $length, $length );
		event_buffer_set_callback( $event,
			array( $this, 'service__queue_request' ),
			null,
			array( $this, 'service__queue_request_error' )
		);
		event_buffer_enable( $event, EV_READ );
	}

	public function service__queue_request_error( $event, $what ) {
		$request_id = array_search( $event, $this->requests_buffering, true );
		$socket = $this->requests_sockets[ $request_id ];
		event_buffer_disable( $event, EV_READ | EV_WRITE );
		event_buffer_free( $event );
		socket_shutdown( $socket );
		socket_close( $socket );
		unset( $this->requests_accepted[ $request_id ] );
		unset( $this->requests_sockets[ $request_id ] );
		unset( $this->requests_buffering[ $request_id ] );
	}

	public function service__queue_request( $event ) {
		$request_id = array_search( $event, $this->requests_buffering, true );
		$this->requests_buffered[ $request_id ] = $event;
		unset( $this->requests_buffering[ $request_id ] );
		$this->service__dispatch_request();
	}

	public function service__accept_offer() {
		$start_time = microtime( true );
		$offer_socket = socket_accept( $this->offer_socket );
		$offer_id = (string) intval( $offer_socket );
		$this->offers_sockets[ $offer_id ] = $offer_socket;
		// Buffer the 4-byte packed PID message
		$event = event_buffer_new( $offer_socket,
			array( $this, 'service__read_offer' ),
			null,
			array( $this, 'service__read_offer_error' ),
			$offer_id
		);
		event_buffer_timeout_set( $event, 1, 1 );
		event_buffer_watermark_set( $event, EV_READ, 4, 4 );
		event_buffer_base_set( $event, $this->event_base );
		event_buffer_enable( $event, EV_READ );
		$this->offers_buffering[ $offer_id ] = $event;
	}

	public function service__read_offer_error( $event, $what, $offer_id ) {
		$socket = $this->offers_sockets[ $offer_id ];
		event_buffer_disable( $event, EV_READ | EV_WRITE );
		event_buffer_free( $event );
		socket_shutdown( $socket );
		socket_close( $socket );
		unset( $this->offers_sockets[ $offer_id ] );
		unset( $this->offers_buffering[ $offer_id ] );
	}

	public function service__read_offer( $offer_event, $offer_id ) {
		event_buffer_disable( $offer_event, EV_READ | EV_WRITE );
		unset( $this->offers_buffering[ $offer_id ] );
		$message = event_buffer_read( $offer_event, 4 );
		$worker_pid = current( unpack( 'N', $message ) );
		if ( ! $this->service__recognize_worker( $worker_pid ) ) {
			event_buffer_free( $offer_event );
			$offer_socket = $this->offers_sockets[ $offer_id ];
			unset( $this->offers_sockets[ $offer_id ] );
			socket_shutdown( $offer_socket );
			socket_close( $offer_socket );
			return;
		}
		$this->workers_ready[ $worker_pid ] = $offer_id;
		$this->offers_workers[ $offer_id ] = $worker_pid;
		$this->offers_buffered[ $offer_id ] = $offer_event;
		event_buffer_set_callback( $offer_event,
			null,
			null,
			array( $this, 'service__ready_worker_error' ),
			$offer_id
		);
		event_buffer_watermark_set( $offer_event, EV_WRITE, 4, 4 );
		event_buffer_enable( $offer_event, EV_WRITE );
		$this->service__dispatch_request();
	}

	private function service__recognize_worker( $worker_pid ) {
		if ( isset( $this->workers_starting[ $worker_pid ] ) ) {
			// This is the first offer from this worker
			unset( $this->workers_starting[ $worker_pid ] );
			// Retire one obsolete worker from the ready list
			foreach ( $this->workers_obsolete as $old_pid => $time ) {
				if ( $old_pid == $worker_pid )
					continue;
				if ( isset( $this->workers_ready[ $old_pid ] ) ) {
					$this->service__retire_worker( $old_pid );
					break;
				}
			}
		}
		if ( isset( $this->workers_obsolete[ $worker_pid ] ) ) {
			$this->service__retire_worker( $worker_pid );
			return false;
		}
		return isset( $this->workers_alive[ $worker_pid ] );
	}

	public function service__ready_worker_error( $event, $what, $worker_id ) {
		event_buffer_disable( $event, EV_READ | EV_WRITE );
		event_buffer_free( $event );
		$socket = $this->offers_sockets[ $offer_id ];
		socket_shutdown( $socket );
		socket_close( $socket );
		unset( $this->workers_ready[ $worker_pid ] );
		unset( $this->offers_sockets[ $offer_id ] );
		unset( $this->offers_workers[ $offer_id ] );
		unset( $this->offers_buffered[ $offer_id ] );
	}

	private function service__dispatch_request() {
		if ( ! $this->requests_buffered )
			return;
		if ( ! $this->workers_ready )
			return;
		// Read a buffered request
		list( $request_id, $request_event ) = $this->take_first( $this->requests_buffered );
		event_buffer_disable( $request_event, EV_READ | EV_WRITE );
		event_buffer_set_callback( $request_event,
			null,
			null,
			array( $this, 'service__working_request_error' ),
			$request_id
		);
		$request_length = $this->requests_lengths[ $request_id ];
		unset( $this->requests_lengths[ $request_id ] );
		$request_message = event_buffer_read( $request_event, $request_length );
		// Write request to ready worker
		list( $worker_pid, $offer_id ) = $this->take_first( $this->workers_ready );
		$offer_event = $this->offers_buffered[ $offer_id ];
		unset( $this->offers_buffered[ $offer_id ] );
		event_buffer_set_callback( $offer_event,
			null,
			array( $this, 'service__write_request_success' ),
			array( $this, 'service__write_request_error' ),
			$offer_id
		);
		$headers = pack( 'N', $request_id ) . pack( 'N', $request_length );
		event_buffer_write( $offer_event, $headers . $request_message );
		$this->offers_written[ $offer_id ] = $offer_event;
		$this->offers_requests[ $offer_id ] = $request_id;
		$this->requests_written[ $request_id ] = $request_event;
		$this->workers_assigned[ $worker_pid ] = $request_id;
	}

	public function service__write_request_error( $offer_event, $what, $offer_id ) {
		// error while sending a request to a worker
		event_buffer_disable( $offer_event, EV_READ | EV_WRITE );
		event_buffer_free( $offer_event );
		$request_id = $this->offers_requests[ $offer_id ];
		$offer_socket = $this->offers_sockets[ $offer_id ];
		socket_shutdown( $offer_socket );
		socket_close( $offer_socket );
		unset( $this->offers_requests[ $offer_id ] );
		unset( $this->offers_sockets[ $offer_id ] );
		unset( $this->offers_written[ $offer_id ] );
		unset( $this->requests_written[ $request_id ] );
		$this->service__close_request( $event, $request_id );
		if ( is_callable( $this->write_request_error_callback ) )
			call_user_func( $this->write_request_error_callback );
	}

	public function service__write_request_success( $offer_event, $offer_id ) {
		event_buffer_disable( $offer_event, EV_READ | EV_WRITE );
		event_buffer_free( $offer_event );
		$offer_socket = $this->offers_sockets[ $offer_id ];
		socket_shutdown( $offer_socket );
		socket_close( $offer_socket );
		$worker_pid = $this->offers_workers[ $offer_id ];
		$request_id = $this->offers_requests[ $offer_id ];
		$request_event = $this->requests_written[ $request_id ];
		unset( $this->workers_ready[ $worker_pid ] );
		unset( $this->offers_sockets[ $offer_id ] );
		unset( $this->offers_written[ $offer_id ] );
		unset( $this->offers_requests[ $offer_id ] );
		$this->requests_working[ $request_id ] = $request_event;
	}

	public function service__accept_response() {
		$response_socket = socket_accept( $this->response_socket );
		$request_id = $this->read_message( $response_socket );
		$response_message = $this->read_message( $response_socket );
		$worker_pid = array_search( $request_id, $this->workers_assigned );
		unset( $this->workers_assigned[ $worker_pid ] );
		socket_shutdown( $response_socket );
		socket_close( $response_socket );
		$this->service__return_response( $request_id, $response_message );
	}

	private function service__return_response( $request_id, $response_message ) {
		if ( array_key_exists( $request_id, $this->requests_working ) ) {
			$length = strlen( $response_message );
			$event = $this->requests_working[ $request_id ];
			event_buffer_disable( $event, EV_READ | EV_WRITE );
			event_buffer_watermark_set( $event, EV_WRITE, 0, 0 );
			event_buffer_timeout_set( $event, 1, 1 );
			event_buffer_set_callback( $event,
				null,
				array( $this, 'service__close_request' ),
				array( $this, 'service__return_response_error' ),
				$request_id
			);
			event_buffer_enable( $event, EV_WRITE );
			$header = pack( 'N', $length );
			event_buffer_write( $event, $header . $response_message );
		}
	}

	public function service__return_response_error( $event, $what, $request_id ) {
		$this->service__close_request( $event, $request_id );
	}

	public function service__working_request_error( $event, $what, $request_id ) {
		$this->service__close_request( $event, $request_id );
	}

	public function service__close_request( $event, $request_id ) {
		event_buffer_disable( $event, EV_READ | EV_WRITE );
		event_buffer_free( $event );
		$request_socket = $this->requests_sockets[ $request_id ];
		socket_shutdown( $request_socket );
		socket_close( $request_socket );
		unset( $this->requests_working[ $request_id ] );
		unset( $this->requests_accepted[ $request_id ] );
		unset( $this->requests_sockets[ $request_id ] );
	}

	public function service__SIGCHLD() {
		// Reap zombies until none remain
		while ( true ) {
			$pid = pcntl_wait( $status, WNOHANG );
			if ( $pid < 1 )
				break;
			$this->service__remove_worker( $pid );
		}
		$this->service__supervise_workers();
	}

	public function service__SIGHUP() {
		// Make old workers obsolete
		$this->workers_obsolete = $this->workers_alive;
		// Spawn replacements
		for ( $i = $this->max_workers; $i > 0; --$i ) {
			if ( $this->service__become_worker() )
				return;
		}
	}

	public function service__SIGINT() {
		$this->received_SIGINT = true;
		if ( $this->service_shutdown ) {
			fwrite( STDERR, "Requests accepted: "
				. count( $this->requests_accepted )
				. " working: "
				. count( $this->requests_working )
				. PHP_EOL );
			return $this->service__continue_shutdown();
		}
		$this->service__begin_shutdown();
	}

	private function service__retire_worker( $pid ) {
		// Do we have a socket to the worker?
		if ( isset( $this->workers_ready[ $pid ] ) ) {
			$socket = $this->workers_ready[ $pid ];
			try {
				@$this->write_message( $socket, 'RETIRE' );
			} catch ( Exception $e ) { }
		}
		$this->service__remove_worker( $pid );
	}

	private function service__remove_worker( $pid ) {
		// Have we assigned a request to this worker?
		if ( isset( $this->workers_assigned[ $pid ] ) ) {
			$request_id = $this->workers_assigned[ $pid ];
			$response = $this->create_error_response();
			$response_message = serialize( $response );
			$this->service__return_response( $request_id, $response_message );
		}
		if ( isset( $this->workers_ready[ $pid ] ) ) {
			$socket = $this->workers_ready[ $pid ];
			@socket_shutdown( $socket );
			@socket_close( $socket );
		}
		unset( $this->workers_ready[ $pid ] );
		unset( $this->workers_obsolete[ $pid ] );
		unset( $this->workers_assigned[ $pid ] );
		unset( $this->workers_starting[ $pid ] );
		unset( $this->workers_alive[ $pid ] );
	}

	private function service__supervise_workers() {
		// Spawn workers to fill empty slots
		while ( count( $this->workers_alive ) < $this->max_workers ) {
			if ( $this->service__become_worker() )
				break;
		}
	}

	private function agent__package_request() {
		$request = array();
		if ( isset( $_SERVER ) )  $request['SERVER']  = $_SERVER;
		if ( isset( $_GET ) )     $request['GET']     = $_GET;
		if ( isset( $_POST ) )    $request['POST']    = $_POST;
		if ( isset( $_COOKIE ) )  $request['COOKIE']  = $_COOKIE;
		if ( isset( $_REQUEST ) ) $request['REQUEST'] = $_REQUEST;
		if ( isset( $_FILES ) )   $request['FILES']   = $_FILES;
		if ( isset( $_SESSION ) ) $request['SESSION'] = $_SESSION;
		if ( isset( $_ENV ) )     $request['ENV']     = $_ENV;
		if ( $_SERVER['REQUEST_METHOD'] === 'POST' && empty( $_POST ) )
			$request['HRPD'] = file_get_contents( 'php://input' );
		return $request;
	}

	private function agent__output_response( $response ) {
		// Send response headers and body
		$headers_sent = array();
		foreach ( $response['headers'] as $header ) {
			// Support for repeated headers. First one replaces, subsequent ones don't.
			list( $header_name, $header_value ) = explode( ':', $header, 2 );
			$replace = ! isset( $headers_sent[ $header_name ] );
			header( $header, $replace );
			$headers_sent[ $header_name ] = true;
		}
		http_response_code( $response['code'] );
		print $response['body'];
		flush();
	}

	private function service__become_worker() {
		$pid = $this->fork_process();
		if ( $pid === 0 ) {
			$this->is_worker = true;
			$this->worker_pid = posix_getpid();
			// The child process ignores SIGINT (small race here)
			pcntl_sigprocmask( SIG_BLOCK, array(SIGINT) );
			// and breaks out of the service event loop
			event_base_loopbreak( $this->event_base );
			// and lets go of the parent's file descriptors
			event_base_reinit( $this->event_base );
			// and the whole event structure
			foreach ( $this->events as $i => $event ) {
				event_del( $event );
				event_free( $event );
				unset( $this->events[$i] );
			}
			$bufferevents = array_merge(
				$this->requests_buffering,
				$this->requests_buffered,
				$this->requests_working
			);
			foreach ( $bufferevents as $event ) {
				event_buffer_disable( $event, EV_READ | EV_WRITE );
				event_buffer_free( $event );
			}
			event_base_free( $this->event_base );
			unset( $this->event_base );
			return true;
		}
		$start_time = microtime( true );
		$this->workers_alive[ $pid ] = $start_time;
		$this->workers_starting[ $pid ] = $start_time;
		return false;
	}

	private function worker__become_intern() {
		$pid = $this->fork_process();
		if ( $pid === 0 ) {
			$this->is_intern = true;
			return true;
		}
		$this->intern_pid = $pid;
		return false;
	}

	private function fork_process() {
		$pid = pcntl_fork();
		if ( $pid === -1 )
			die( "Fork failure\n" );
		return $pid;
	}

	private function service__begin_shutdown() {
		fwrite( STDERR, "Service shutting down" . PHP_EOL );
		// Stop listening for events on the request port
		event_del( $this->events['request'] );
		// Release the port so the next service can bind it
		socket_shutdown( $this->request_socket, 0 );
		$this->service_shutdown = true;
	}

	private function service__continue_shutdown() {
		// Delay shutdown until all accepted requests are completed
		if ( $this->requests_accepted )
			return;
		event_base_loopbreak( $this->event_base );
		foreach ( $this->workers_alive as $pid => $time )
			posix_kill( $pid, SIGKILL );
		fwrite( STDERR, "Shutdown complete" . PHP_EOL );
		if ( $this->received_SIGINT ) {
			pcntl_signal( SIGINT, SIG_DFL );
			posix_kill( posix_getpid(), SIGINT );
			// The following unreachable line is merely informative
			exit(130);
		}
		exit(0);
	}

	public function fork() {
		// Prefork service did not start so proceed as a typical web request
		if ( ! $this->is_service )
			return;
		// Check requirements
		if ( ob_get_level() > 1 )
			die( 'Prefork Error: other output buffers already started in application loader.' );
		if ( is_callable( $this->prefork_callback ) )
			call_user_func( $this->prefork_callback );
		// The worker stays in this loop, spawning slaves
		while ( true ) {
			// Reap zombies
			while ( true ) {
				$pid = pcntl_wait( $status, WNOHANG );
				if ( $pid < 1 )
					break;
			}
			// Multi-intern workers must avoid overloading RAM
			if ( !$this->single_interns )
				$this->wait_for_resources();
			$request_message = $this->worker__receive_request();
			if ( empty( $request_message ) )
				continue;
			if ( $this->worker__become_intern() )
				break;
			unset( $request_message );
			if ( $this->single_interns ) {
				pcntl_waitpid( $this->intern_pid, $status );
				if ( $status == 0 )
					continue;
				$response = $this->create_error_response();
				$this->worker__send_response_to_service( $response );
			}
		}
		// Interns only past this point
		$request = unserialize( $request_message );
		unset( $request_message );
		$this->intern__prepare_request( $request );
		if ( is_callable( $this->postfork_callback ) )
			call_user_func( $this->postfork_callback );
	}

	private function wait_for_resources( $interval = 10000 ) {
		while ( ! $this->has_free_resources() )
			usleep( $interval );
	}

	private function has_free_resources() {
		$meminfo = file_get_contents( '/proc/meminfo' ); // Linux
		preg_match( '/MemTotal:\s+(\d+)/', $meminfo, $matches );
		$total = $matches[1];
		preg_match( '/MemFree:\s+(\d+)/', $meminfo, $matches );
		$free = $matches[1];
		$free_ram = $free / $total;
		return ( $free_ram > $this->min_free_ram );
	}

	private function intern__prepare_request( $request ) {
		// Prepare request variables
		if ( isset( $request['SERVER'] ) )  $_SERVER  = $request['SERVER'];
		if ( isset( $request['GET'] ) )     $_GET     = $request['GET'];
		if ( isset( $request['POST'] ) )    $_POST    = $request['POST'];
		if ( isset( $request['COOKIE'] ) )  $_COOKIE  = $request['COOKIE'];
		if ( isset( $request['REQUEST'] ) ) $_REQUEST = $request['REQUEST'];
		if ( isset( $request['FILES'] ) )   $_FILES   = $request['FILES'];
		if ( isset( $request['SESSION'] ) ) $_SESSION = $request['SESSION'];
		if ( isset( $request['ENV'] ) )     $_ENV     = $request['ENV'];
		if ( isset( $request['HRPD'] ) )    $GLOBALS['HTTP_RAW_POST_DATA'] = $request['HRPD'];
		// Prepare to collect output
		ob_start( array( $this, 'intern__ob_handler' ) );
	}

	public function intern__ob_handler( $output ) {
		$response = array(
			'code'    => http_response_code(),
			'headers' => headers_list(),
			'body'    => $output,
		);
		$this->intern__send_response_to_service( $response );
		return '';
	}

	private function agent__transact_with_service( $request ) {
		$socket = socket_create( AF_INET, SOCK_STREAM, 0 );
		socket_set_option( $socket, SOL_SOCKET, SO_SNDTIMEO, array( 'sec' => 0, 'usec' => 10000 ) );
		socket_set_option( $socket, SOL_SOCKET, SO_RCVTIMEO, array( 'sec' => 30, 'usec' => 0 ) );
		$connected = @socket_connect( $socket, $this->request_address, $this->request_port );
		if ( ! $connected )
			return false;
		$request_message = serialize( $request );
		try {
			$sent = $this->write_message( $socket, $request_message );
			$response_message = $this->read_message( $socket );
			$response = unserialize( $response_message );
			return $response;
		} catch ( Exception $e ) {
			return $this->create_error_response();
		}
	}

	private function service__create_sockets() {
		// Prepare to accept connections from Agents
		$this->request_socket = socket_create( AF_INET, SOCK_STREAM, 0 );
		socket_set_option( $this->request_socket, SOL_SOCKET, SO_REUSEADDR, 1);
		// bind(2) ensures we don't start multiple services
		if ( ! socket_bind( $this->request_socket, $this->request_address, $this->request_port ) ) {
			socket_close( $this->request_socket );
			return false;
		}
		socket_listen( $this->request_socket, $this->request_backlog );
		// Prepare to accept connections from Workers ready for requests
		$this->offer_socket = socket_create( AF_INET, SOCK_STREAM, 0 );
		socket_set_option($this->offer_socket, SOL_SOCKET, SO_REUSEADDR, 1);
		while ( ! socket_bind( $this->offer_socket, $this->offer_address, $this->offer_port ) )
			++$this->offer_port;
		socket_listen( $this->offer_socket, $this->offer_backlog );
		// Prepare to accept connections from Workers ready with responses
		$this->response_socket = socket_create( AF_INET, SOCK_STREAM, 0 );
		socket_set_option($this->response_socket, SOL_SOCKET, SO_REUSEADDR, 1);
		while ( ! socket_bind( $this->response_socket, $this->response_address, $this->response_port ) )
			++$this->response_port;
		socket_listen( $this->response_socket, $this->response_backlog );
		return true;
	}

	private function worker__receive_request() {
		$socket = socket_create( AF_INET, SOCK_STREAM, 0 );
		$message = pack( 'N', $this->worker_pid );
		socket_connect( $socket, $this->offer_id, $this->offer_port );
		socket_send( $socket, $message, 4, 0 );
		socket_recv( $socket, $buffer, 4, MSG_WAITALL );
		$message = current( unpack( 'N', $buffer ) );
		if ( ! $message )
			$this->worker__retire();
		$this->request_id = $message;
		$request_message = $this->read_message( $socket );
		socket_shutdown( $socket );
		socket_close( $socket );
		return $request_message;
	}

	private function worker__retire() {
		if ( is_callable( $this->worker__retire_callback ) )
			call_user_func( $this->worker__retire_callback );
		exit(0);
	}

	public function worker__send_response_to_service( $response ) {
		return $this->intern__send_response_to_service( $response );
	}

	public function intern__send_response_to_service( $response ) {
		$socket = socket_create( AF_INET, SOCK_STREAM, 0 );
		socket_connect( $socket, $this->response_address, $this->response_port );
		$this->write_message( $socket, $this->request_id );
		$response_message = serialize( $response );
		$this->write_message( $socket, $response_message );
		socket_shutdown( $socket );
		socket_close( $socket );
	}

	/***** Internal methods *****/

	private function event_add( $name, $fd, $flags, $callback, $timeout = -1 ) {
		$event = event_new();
		event_set( $event, $fd, $flags, array( $this, $callback ) );
		event_base_set( $event, $this->event_base );
		event_add( $event, $timeout );
		$this->events[ $name ] = $event;
	}

	private function read_message( $socket ) {
		// Receive header indicating message length
		$recv = socket_recv( $socket, $header, 4, MSG_WAITALL );
		if ( strlen( $header ) !== 4 )
			throw new Exception( "Prefork::read_message() failed receiving header" );
		$length = current( unpack( 'N', $header ) );
		$recv = socket_recv( $socket, $message, $length, MSG_WAITALL );
		if ( strlen( $message ) !== $length )
			throw new Exception( "Prefork::read_message() failed receiving message" );
		return $message;
	}

	private function write_message( $socket, $message, $debug = false ) {
		$length = strlen( $message );
		$header = pack( 'N', $length );
		$header_sent = socket_send( $socket, $header, 4, 0 ) === 4;
		if ( ! $header_sent )
			throw new Exception( "Prefork::write_message() failed sending header" );
		$message_sent = socket_send( $socket, $message, $length, 0 ) === $length;
		if ( ! $message_sent )
			throw new Exception( "Prefork::write_message() failed sending message" );
		return true;
	}

	private function create_error_response() {
		return array(
			'code' => 500,
			'headers' => array(),
			'body' => 'Internal server error',
		);
	}

	private function take_first( &$array ) {
		reset( $array );
		list( $key, $value ) = each( $array );
		unset( $array[ $key ] );
		return array( $key, $value );
	}
}

