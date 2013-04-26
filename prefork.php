<?php

class Prefork {
	// Configuration
	public $prefork_callback;
	public $postfork_callback;
	public $max_workers = 4;

	// Sockets config
	public $frontend_address = "127.0.0.1";
	public $frontend_port = 8081;
	public $frontend_backlog = 64;
	public $backend_request_address = "127.0.0.1";
	public $backend_request_port = 8082;
	public $backend_request_backlog = 64;
	public $backend_response_address = "127.0.0.1";
	public $backend_response_port = 8083;
	public $backend_response_backlog = 64;

	// Sockets for listening
	private $frontend_socket;
	private $backend_request_socket;
	private $backend_response_socket;

	// Sockets for accepted connections
	private $ready_worker_sockets;

	// Service state
	private $is_service;
	private $event_base;
	private $pending_requests = 0;
	private $waiting_request_sockets = array();
	private $pending_request_sockets = array();
	private $worker_pids = array();
	private $workers_retiring = array();

	// Worker/intern state
	private $return_address;

	public function __construct() {
		if ( version_compare( PHP_VERSION, '5.4', '<' ) )
			die( 'Error: Prefork requires PHP 5.4 for http_response_code().' . PHP_EOL );
	}

	public function become_agent() {
		$request = array();
		if ( isset( $_SERVER ) )  $request['SERVER']  = $_SERVER;
		if ( isset( $_GET ) )     $request['GET']     = $_GET;
		if ( isset( $_POST ) )    $request['POST']    = $_POST;
		if ( isset( $_COOKIE ) )  $request['COOKIE']  = $_COOKIE;
		if ( isset( $_REQUEST ) ) $request['REQUEST'] = $_REQUEST;
		if ( isset( $_FILES ) )   $request['FILES']   = $_FILES;
		if ( isset( $_SESSION ) ) $request['SESSION'] = $_SESSION;
		if ( isset( $_ENV ) )     $request['ENV']     = $_ENV;
		if ( empty( $_POST ) && isset( $HTTP_RAW_POST_DATA ) )
			$request['HRPD'] = $HTTP_RAW_POST_DATA;
		// Did we miss anything?

		$response = $this->agent__transact_with_service( $request );

		if ( $response === false )
			return false;

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

		return true;
	}

	public function become_service() {
		if ( ! $this->service__create_sockets() )
			return false;

		$this->is_service = true;

		$this->event_base = event_base_new();

		// Install signal handler for worker exits
		$this->event_SIGCHLD = event_new();
		event_set( $this->event_SIGCHLD, SIGCHLD, EV_SIGNAL | EV_PERSIST,
			array( $this, 'service__handle_SIGCHLD' ) );
		event_base_set( $this->event_SIGCHLD, $this->event_base );
		event_add( $this->event_SIGCHLD );
		$this->events[] = $this->event_SIGCHLD;

		// Install signal handler for reloading app code
		$this->event_SIGHUP = event_new();
		event_set( $this->event_SIGHUP, SIGHUP, EV_SIGNAL | EV_PERSIST,
			array( $this, 'service__handle_SIGHUP' ) );
		event_base_set( $this->event_SIGHUP, $this->event_base );
		event_add( $this->event_SIGHUP );
		$this->events[] = $this->event_SIGHUP;

		// Install signal handler for reloading app code
		$this->event_SIGINT = event_new();
		event_set( $this->event_SIGINT, SIGINT, EV_SIGNAL | EV_PERSIST,
			array( $this, 'service__handle_SIGINT' ) );
		event_base_set( $this->event_SIGINT, $this->event_base );
		event_add( $this->event_SIGINT );
		$this->events[] = $this->event_SIGINT;

		// Install recurring worker supervision function
		$this->event_supervise_workers = event_new();
		event_set( $this->event_supervise_workers, 0, EV_TIMEOUT | EV_PERSIST,
			array( $this, 'service__supervise_workers' ) );
		event_base_set( $this->event_supervise_workers, $this->event_base );
		event_add( $this->event_supervise_workers, 500000 );
		$this->events[] = $this->event_supervise_workers;

		// Install frontend socket listener
		$this->event_frontend = event_new();
		event_set( $this->event_frontend, $this->frontend_socket, EV_READ | EV_PERSIST,
			array( $this, 'service__handle_frontend' ) );
		event_base_set( $this->event_frontend, $this->event_base );
		event_add( $this->event_frontend );
		$this->events[] = $this->event_frontend;

		// Install backend request socket listener
		$this->event_backend_request = event_new();
		event_set( $this->event_backend_request, $this->backend_request_socket, EV_READ | EV_PERSIST,
			array( $this, 'service__handle_backend_request' ) );
		event_base_set( $this->event_backend_request, $this->event_base );
		event_add( $this->event_backend_request );
		$this->events[] = $this->event_backend_request;

		// Install backend response socket listener
		$this->event_backend_response = event_new();
		event_set( $this->event_backend_response, $this->backend_response_socket, EV_READ | EV_PERSIST,
			array( $this, 'service__handle_backend_response' ) );
		event_base_set( $this->event_backend_response, $this->event_base );
		event_add( $this->event_backend_response );
		$this->events[] = $this->event_backend_response;

		// The service stays in this call while workers return from it
		event_base_loop( $this->event_base );

		// The service is running
		return true;
	}

	public function service__handle_frontend() {
		$request_socket = socket_accept( $this->frontend_socket );
		$return_address = (string) intval( $request_socket );
		$this->waiting_request_sockets[ $return_address ] = $request_socket;
		$this->service__dispatch_request();
	}

	public function service__handle_backend_request() {
		$worker_socket = socket_accept( $this->backend_request_socket );
		$worker_pid = $this->read_message( $worker_socket );
		if ( isset( $this->workers_retiring[ $worker_pid ] ) ) {
			$this->write_message( $worker_socket, 'RETIRE' );
			unset( $this->workers_retiring[ $worker_pid ] );
			return;
		}
		$this->ready_worker_sockets[ $worker_pid ] = $worker_socket;
		$this->service__dispatch_request();
	}

	private function service__dispatch_request() {
		if ( $this->waiting_request_sockets && $this->ready_worker_sockets ) {
			reset( $this->waiting_request_sockets );
			list( $return_address, $request_socket ) = each( $this->waiting_request_sockets );
			unset( $this->waiting_request_sockets[ $return_address ] );
			list( $worker_pid, $worker_socket ) = each( $this->ready_worker_sockets );
			unset( $this->ready_worker_sockets[ $worker_pid ] );
			$request_message = $this->read_message( $request_socket );
			$this->write_message( $worker_socket, $return_address );
			$this->write_message( $worker_socket, $request_message );
			socket_shutdown( $worker_socket );
			socket_close( $worker_socket );
			$this->pending_request_sockets[ $return_address ] = $request_socket;
		}
	}

	public function service__handle_backend_response() {
		$response_socket = socket_accept( $this->backend_response_socket );
		$return_address = $this->read_message( $response_socket );
		$response_message = $this->read_message( $response_socket );
		socket_shutdown( $response_socket );
		socket_close( $response_socket );
		if ( array_key_exists( $return_address, $this->pending_request_sockets ) ) {
			$frontend_socket = $this->pending_request_sockets[ $return_address ];
			$this->write_message( $frontend_socket, $response_message );
			socket_shutdown( $frontend_socket );
			socket_close( $frontend_socket );
			unset( $this->pending_request_sockets[ $return_address ] );
		}
	}

	public function service__handle_SIGCHLD() {
		while ( true ) {
			$pid = pcntl_wait( $status, WNOHANG );
			if ( $pid < 1 )
				break;
			if ( isset( $this->ready_worker_sockets[ $pid ] ) ) {
				socket_close( $this->ready_worker_sockets[ $pid ] );
				unset( $this->ready_worker_sockets[ $pid ] );
			}
			if ( isset( $this->workers_retiring[ $pid ] ) )
				unset( $this->workers_retiring[ $pid ] );
			unset( $this->worker_pids[ $pid ] );
			$this->service__supervise_workers();
		}
	}

	public function service__handle_SIGHUP() {
		$this->workers_retiring = array_flip( array_keys( $this->worker_pids ) );
	}

	public function service__handle_SIGINT() {
		$this->service__shutdown();
		event_base_loopexit( $this->event_base );
		exit;
	}

	public function service__supervise_workers() {
		while ( count( $this->worker_pids ) < $this->max_workers ) {
			if ( $this->service__become_worker() ) {
				break;
			}
		}
	}

	private function service__become_worker() {
		$pid = $this->fork_process();
		if ( $pid === 0 ) {
			// The child process breaks out of the service event loop
			event_base_loopbreak( $this->event_base );
			// and lets go of the parent's file descriptors
			event_base_reinit( $this->event_base );
			// and the whole event structure
			foreach ( $this->events as $i => $event ) {
				event_del( $event );
				event_free( $event );
				unset( $this->events[$i] );
			}
			event_base_free( $this->event_base );
			unset( $this->event_base );
			return true;
		}
		$this->worker_pids[ $pid ] = microtime();
		return false;
	}

	private function worker__become_intern() {
		$pid = $this->fork_process();
		if ( $pid === 0 )
			return true;
		$this->intern_pid = $pid;
		return false;
	}

	private function fork_process() {
		$pid = pcntl_fork();
		if ( $pid === -1 )
			die( "Fork failure\n" );
		return $pid;
	}

	public function service__shutdown() {
		print "Service shutting down" . PHP_EOL;
		socket_shutdown( $this->frontend_socket );
		// TODO: safely return all responses before killing workers
		// Tell all workers they need to exit gracefully
		foreach ( $this->worker_pids as $pid => $time )
			posix_kill( $pid, SIGKILL ); // SIGHUP );
		socket_shutdown( $this->backend_request_socket );
		socket_shutdown( $this->backend_response_socket );
		socket_close( $this->frontend_socket );
		socket_close( $this->backend_request_socket );
		socket_close( $this->backend_response_socket );
	}

	public function fork() {
		// Check requirements
		if ( ob_get_level() > 1 )
			die( 'Prefork Error: other output buffers already started in application loader.' );

		// Prefork service did not start so proceed as a typical web request
		if ( ! $this->is_service )
			return;

		if ( is_callable( $this->prefork_callback ) )
			call_user_func( $this->prefork_callback );

		// The worker stays in this loop, spawning slaves
		while ( true ) {
			$request_message = $this->worker__receive_request();
			if ( empty( $request_message ) || $request_message === 'RETIRE' )
				exit;

			if ( $this->worker__become_intern() )
				break; // Child process assumes request vars, runs app & sends response

			// Block until the child exits
			pcntl_waitpid( $this->intern_pid, $status );
			if ( $status > 0 ) {
				$response = $this->create_error_response();
				$this->worker__send_response_to_service( $response );
			}
		}

		$request = unserialize( $request_message );

		// Interns only past this point
		if ( isset( $request['SERVER'] ) )  $_SERVER  = $request['SERVER'];
		if ( isset( $request['GET'] ) )     $_GET     = $request['GET'];
		if ( isset( $request['POST'] ) )    $_POST    = $request['POST'];
		if ( isset( $request['COOKIE'] ) )  $_COOKIE  = $request['COOKIE'];
		if ( isset( $request['REQUEST'] ) ) $_REQUEST = $request['REQUEST'];
		if ( isset( $request['FILES'] ) )   $_FILES   = $request['FILES'];
		if ( isset( $request['SESSION'] ) ) $_SESSION = $request['SESSION'];
		if ( isset( $request['ENV'] ) )     $_ENV     = $request['ENV'];
		if ( isset( $request['HRPD'] ) )    $HTTP_RAW_POST_DATA = $request['HRPD'];

		if ( is_callable( $this->postfork_callback ) )
			call_user_func( $this->postfork_callback );

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

	public function agent__transact_with_service( $request ) {
		$socket = socket_create( AF_INET, SOCK_STREAM, 0 );
		socket_set_option( $socket, SOL_SOCKET, SO_SNDTIMEO, array( 'sec' => 0, 'usec' => 5000 ) );
		socket_set_option( $socket, SOL_SOCKET, SO_RCVTIMEO, array( 'sec' => 30, 'usec' => 0 ) );
		$connected = @socket_connect( $socket, $this->frontend_address, $this->frontend_port );
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

	public function service__create_sockets() {
		// Prepare to accept connections from Agents
		$this->frontend_socket = socket_create( AF_INET, SOCK_STREAM, 0 );
		socket_set_option($this->frontend_socket, SOL_SOCKET, SO_REUSEADDR, 1);
		// This bind(2) system call ensures we can't start multiple services per address:port
		if ( ! @socket_bind( $this->frontend_socket, $this->frontend_address, $this->frontend_port ) ) {
			socket_close( $this->frontend_socket );
			return false;
		}
		socket_listen( $this->frontend_socket, $this->frontend_backlog );

		// Prepare to accept connections from Workers ready for requests
		$this->backend_request_socket = socket_create( AF_INET, SOCK_STREAM, 0 );
		socket_set_option($this->backend_request_socket, SOL_SOCKET, SO_REUSEADDR, 1);
		socket_bind( $this->backend_request_socket, $this->backend_request_address, $this->backend_request_port );
		socket_listen( $this->backend_request_socket, $this->backend_request_backlog );

		// Prepare to accept connections from Workers ready with responses
		$this->backend_response_socket = socket_create( AF_INET, SOCK_STREAM, 0 );
		socket_set_option($this->backend_response_socket, SOL_SOCKET, SO_REUSEADDR, 1);
		socket_bind( $this->backend_response_socket, $this->backend_response_address, $this->backend_response_port );
		socket_listen( $this->backend_response_socket, $this->backend_response_backlog );

		return true;
	}

	public function worker__receive_request() {
		$socket = socket_create( AF_INET, SOCK_STREAM, 0 );
		socket_connect( $socket, $this->backend_request_address, $this->backend_request_port );
		$this->write_message( $socket, (string) posix_getpid() );
		$message = $this->read_message( $socket );
		if ( $message === 'RETIRE' )
			exit;
		$this->return_address = $message;
		$request_message = $this->read_message( $socket );
		socket_shutdown( $socket );
		socket_close( $socket );
		return $request_message;
	}

	public function worker__send_response_to_service( $response ) {
		return $this->intern__send_response_to_service( $response );
	}

	public function intern__send_response_to_service( $response ) {
		$socket = socket_create( AF_INET, SOCK_STREAM, 0 );
		socket_connect( $socket, $this->backend_response_address, $this->backend_response_port );
		$this->write_message( $socket, $this->return_address );
		$response_message = serialize( $response );
		$this->write_message( $socket, $response_message );
		socket_shutdown( $socket );
		socket_close( $socket );
	}

	/***** Internal methods *****/

	private function read_message( $socket ) {
		socket_set_block( $socket );
		socket_recv( $socket, $header, 4, MSG_WAITALL );
		if ( strlen( $header ) !== 4 )
			throw new Exception( "Prefork::read_message() failed receiving header" );
		$length = current( unpack( 'N', $header ) );
		socket_recv( $socket, $message, $length, MSG_WAITALL );
		if ( strlen( $message ) !== $length )
			throw new Exception( "Prefork::read_message() failed receiving message" );
		return $message;
	}

	private function write_message( $socket, $message ) {
		socket_set_nonblock( $socket );
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
}

