<?php

/**
 * Prefork static interface
 */
class Prefork {
	private static $service; // Instance of Prefork_Service
	private static $agent;   // Instance of Prefork_Agent
	private static $ini_array = array();
	private static $ini_file = '';

	// No instances allowed
	private function __construct() {}

	public static function use_ini_array( array $ini_array ) {
		self::$ini_array = $ini_array;
	}

	public static function use_ini_file( $ini_file ) {
		self::$ini_file = $ini_file;
	}

	public static function start_service() {
		self::$service = new Prefork_Service( self::$ini_array, self::$ini_file );
		return self::$service->start();
	}

	public static function start_agent() {
		self::$agent = new Prefork_Agent( self::$ini_array, self::$ini_file );
		return self::$agent->start();
	}

	public static function start_status_agent() {
		self::$agent = new Prefork_Status_Agent( self::$ini_array, self::$ini_file );
		return self::$agent->start();
	}

	public static function fork() {
		if ( self::$service->worker )
			self::$service->worker->fork();
	}

	public static function finish_request() {
		if ( self::$service->worker->intern )
			self::$service->worker->intern->finish_request();
	}
}

/**
 * Contains functions used by more than one role
 */
class Prefork_Role {
	protected $ini_file;

	public $max_message_length = 16777216; // 16MB
	public $gateway_timeout = 120; // seconds

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

	/**
	 * Load options from $ini_array, then override with $ini_file options.
	 * Load only those options specified in the role's $ini_options array.
	 */
	public function __construct( $ini_array = array(), $ini_file = '' ) {
		if ( is_object( $ini_array ) )
			$ini_array = get_object_vars( $ini_array );
		if ( !empty( $ini_array ) ) {
			$this->load_ini_array( $ini_array, $this->ini_options );
		}
		if ( !empty( $ini_file ) ) {
			$this->ini_file = $ini_file;
			$this->load_ini_file( $this->ini_options );
		}
	}

	protected function load_ini_file( array $options ) {
		if ( file_exists( $this->ini_file ) ) {
			$ini_file_array = parse_ini_file( $this->ini_file );
			$this->load_ini_array( $ini_file_array, $options );
		}
	}

	private function load_ini_array( $ini_array, $options ) {
		foreach ( $options as $option )
			if ( isset( $ini_array[ $option ] ) )
				$this->{$option} = $ini_array[ $option ];
	}

	protected function fork_process() {
		$pid = pcntl_fork();
		if ( $pid === -1 )
			die( "Fork failure\n" );
		if ( $pid === 0 ) {
			// Re-seed the child's PRNGs
			srand();
			mt_srand();
		}
		return $pid;
	}

	protected function read_message( $socket ) {
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

	protected function write_message( $socket, $message, $debug = false ) {
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

	protected function create_error_response() {
		return array(
			'code' => 500,
			'headers' => array(),
			'body' => 'Internal server error',
		);
	}
}

class Prefork_Agent extends Prefork_Role {
	protected $ini_options = array(
		'request_address',
		'request_port',
		'gateway_timeout',
	);

	public function start() {
		$request = $this->package_request();
		$response = $this->transact_with_service( $request );
		// FALSE means the request was not processed at all
		if ( $response === false )
			return false;
		$this->output_response( $response );
		return true;
	}

	private function package_request() {
		$request = array();
		if ( isset( $_SERVER ) )  $request['SERVER']  = $_SERVER;
		if ( isset( $_GET ) )     $request['GET']     = $_GET;
		if ( isset( $_POST ) )    $request['POST']    = $_POST;
		if ( isset( $_COOKIE ) )  $request['COOKIE']  = $_COOKIE;
		if ( isset( $_REQUEST ) ) $request['REQUEST'] = $_REQUEST;
		if ( isset( $_FILES ) )   $request['FILES']   = $_FILES;
		if ( isset( $_SESSION ) ) $request['SESSION'] = $_SESSION;
		if ( isset( $_ENV ) )     $request['ENV']     = $_ENV;
		if ( $_SERVER['REQUEST_METHOD'] === 'POST' )
			$request['HRPD'] = file_get_contents( 'php://input' );
		return $request;
	}

	private function transact_with_service( $request ) {
		$socket = socket_create( AF_INET, SOCK_STREAM, 0 );
		socket_set_option( $socket, SOL_SOCKET, SO_SNDTIMEO, array( 'sec' => 0, 'usec' => 10000 ) );
		socket_set_option( $socket, SOL_SOCKET, SO_RCVTIMEO, array( 'sec' => $this->gateway_timeout, 'usec' => 0 ) );
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
			error_log( __FUNCTION__ . ' caught Exception: ' . $e->getMessage() );
			return $this->create_error_response();
		}
	}

	protected function output_response( $response ) {
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
}

class Prefork_Status_Agent extends Prefork_Agent {
	public function start() {
		$response = $this->request_status_from_service();
		if ( $response === false )
			return false;
		$this->output_response( $response );
		return true;
	}

	private function request_status_from_service() {
		$socket = socket_create( AF_INET, SOCK_STREAM, 0 );
		socket_set_option( $socket, SOL_SOCKET, SO_SNDTIMEO, array( 'sec' => 0, 'usec' => 10000 ) );
		socket_set_option( $socket, SOL_SOCKET, SO_RCVTIMEO, array( 'sec' => 30, 'usec' => 0 ) );
		$connected = @socket_connect( $socket, $this->request_address, $this->request_port );
		if ( ! $connected )
			return false;
		socket_send( $socket, 'stat', 4, 0 );
		$response_message = $this->read_message( $socket );
		$response = unserialize( $response_message );
		return $response;
	}
}

class Prefork_Service extends Prefork_Role {
	public $pidfile;
	public $daemonize = true;

	// Resource limits for multi-intern workers to observe
	public $min_free_ram = 0.20; // Minimum free RAM (MemFree / MemTotal)

	// Service config
	public $heartbeat_bpm = 100; // Heartbeats per minute
	public $heartbeat_callback;  // Optional: callback to run on heartbeat
	public $max_workers = 1;     // How many workers should load the app
	public $single_interns = true; // Limit interns to one per worker
	public $prefork_callback;    // Optional: to be run before forking
	public $postfork_callback;   // Optional: to be run after forking

	protected $ini_options = array(
		'pidfile', 'daemonize',
		'min_free_ram',
		'heartbeat_bpm', 'heartbeat_callback',
		'prefork_callback', 'postfork_callback',
		'max_workers', 'single_interns',
		'request_address', 'request_port', 'request_backlog',
		'offer_address', 'offer_port', 'offer_backlog',
		'response_address', 'response_port', 'response_backlog',
		'gateway_timeout',
	);
	// Options which are reloaded on SIGHUP
	private $ini_options_HUP = array(
		'min_free_ram',
		'heartbeat_bpm', 'heartbeat_callback',
		'max_workers', 'single_interns',
	);

	// Sockets for listening
	private $request_socket;  // for Agents transacting with Service 
	private $offer_socket;    // for Workers offering to handle Requests
	private $response_socket; // for Interns/Workers returning Responses

	// Service state
	public $event_base;
	private $received_SIGINT; // true: reminder to send self SIGINT on exit
	private $received_SIGTERM; // true: reminder to send self SIGTERM on exit
	private $service_shutdown; // true: service is shutting down
	private $workers_alive = array();       // worker_pid => time
	private $workers_starting = array();    // worker_pid => time
	private $workers_ready = array();       // worker_pid => offer_id
	private $workers_obsolete = array();    // worker_pid => time
	private $offers_sockets = array();      // offer_id => socket
	private $offers_accepted = array();     // offer_id => event buffer
	private $offers_waiting = array();      // offer_id => event buffer
	private $offers_written = array();      // offer_id => event buffer
	private $offers_requests = array();     // offer_id => request_id
	private $offers_workers = array();      // offer_id => worker_pid
	private $requests_started = array();    // request_id => time
	private $requests_sockets = array();    // request_id => socket
	private $requests_lengths = array();    // request_id => int
	private $requests_accepted = array();   // request_id => event buffer
	private $requests_buffered = array();   // request_id => event buffer
	private $requests_written = array();    // request_id => event buffer
	private $requests_working = array();    // request_id => event buffer
	private $requests_assigned = array();   // request_id => worker_pid
	private $responses_sockets = array();   // response_id => socket
	private $responses_lengths = array();   // response_id => int
	private $responses_requests = array();  // response_id => request_id
	private $responses_accepted = array();  // response_id => event buffer
	private $responses_buffering = array(); // response_id => event buffer

	private $method_calls = array(); // function => int

	public $worker_pid;
	public $worker;

	public function start() {
		if ( version_compare( PHP_VERSION, '5.4', '<' ) )
			$this->fail( 'Error: Prefork requires PHP 5.4 for http_response_code().' );
		if ( ! $this->test_headers() )
			$this->fail( 'Error: Prefork requires header() and headers_list(). Use CGI or patched CLI.' );
		if ( ! function_exists( 'event_base_reinit' ) )
			$this->fail( 'Error: Prefork requires libevent with event_base_reinit(). Upgrade to 0.1.0.' );
		if ( ! $this->create_sockets() )
			$this->fail( 'Error: Prefork failed to bind service socket.' );
		define( 'PREFORK_SERVICE', true );
		if ( $this->daemonize ) {
			if ( defined( 'STDIN' ) ) {
				fclose( STDIN );
				fclose( STDOUT );
				fclose( STDERR );
			}
			// Detach from calling process
			$pid = pcntl_fork();
			if ( $pid < 0 )
				exit(1);
			if ( $pid > 0 )
				exit(0);
			@umask(0);
			@posix_setsid();
		}
		$this->create_pidfile();
		// The service stays in this call while workers return from it
		$this->event_loop();
		return true;
	}

	private function fail( $message ) {
		print $message . PHP_EOL;
		exit( 1 );
	}

	private function test_headers() {
		$test_header_name = 'X-Prefork-Test-Header';
		$test_header = "$test_header_name: OK";
		header( $test_header );
		$result = in_array( $test_header, headers_list() );
		header_remove( $test_header_name );
		return $result;
	}

	private function create_pidfile() {
		if ( ! isset( $this->pidfile ) )
			return;
		$pid = posix_getpid();
		file_put_contents( $this->pidfile, $pid . PHP_EOL );
	}

	private function unlink_pidfile() {
		if ( ! isset( $this->pidfile ) )
			return;
		unlink( $this->pidfile );
	}

	private function event_loop() {
		// Set up the event loop
		$this->event_base = event_base_new();
		// Signal handlers
		$this->event_add( 'SIGCHLD', SIGCHLD, EV_SIGNAL | EV_PERSIST,
			'SIGCHLD' );
		$this->event_add( 'SIGHUP', SIGHUP, EV_SIGNAL | EV_PERSIST,
			'SIGHUP' );
		$this->event_add( 'SIGINT', SIGINT, EV_SIGNAL | EV_PERSIST,
			'SIGINT' );
		$this->event_add( 'SIGTERM', SIGTERM, EV_SIGNAL | EV_PERSIST,
			'SIGTERM' );
		// Socket handlers
		$this->event_add( 'request', $this->request_socket,
			EV_READ | EV_PERSIST, 'accept_request' );
		$this->event_add( 'offer', $this->offer_socket,
			EV_READ | EV_PERSIST, 'accept_offer' );
		$this->event_add( 'response', $this->response_socket,
			EV_READ | EV_PERSIST, 'accept_response' );
		// Heartbeat
		$timeout_microseconds = intval( 60e6 / $this->heartbeat_bpm );
		$this->event_add( 'heartbeat', 0, EV_TIMEOUT | EV_PERSIST,
			'heartbeat', $timeout_microseconds );
		// Initial worker start (run only once)
		$this->event_add( 'start', 0, EV_TIMEOUT,
			'supervise_workers', 1 );
		event_base_loop( $this->event_base );
	}

	public function heartbeat() {
		$this->log_method_call( __FUNCTION__ );
		if ( is_callable( $this->heartbeat_callback ) )
			call_user_func( $this->heartbeat_callback );
		if ( $this->service_shutdown )
			$this->continue_shutdown();
		$this->supervise_workers();
		$this->supervise_requests();
	}

	public function accept_request() {
		$this->log_method_call( __FUNCTION__ );
		$start_time = microtime( true );
		$socket = socket_accept( $this->request_socket );
		$request_id = (string) intval( $socket );
		$this->requests_sockets[ $request_id ] = $socket;
		$this->requests_started[ $request_id ] = $start_time;
		asort( $this->requests_started );
		// Buffer the 4-byte header
		$event = event_buffer_new( $socket,
			array( $this, 'read_request_header' ),
			null,
			array( $this, 'read_request_header_error' ),
			$request_id
		);
		event_buffer_watermark_set( $event, EV_READ, 4, 4 );
		event_buffer_base_set( $event, $this->event_base );
		event_buffer_timeout_set( $event, 1, 1 );
		event_buffer_enable( $event, EV_READ );
		$this->requests_accepted[ $request_id ] = $event;
	}

	public function read_request_header_error( $request_event, $what, $request_id ) {
		$this->log_method_call( __FUNCTION__ );
		$request_socket = $this->requests_sockets[ $request_id ];
		event_buffer_disable( $request_event, EV_READ | EV_WRITE );
		event_buffer_free( $request_event );
		socket_shutdown( $request_socket );
		socket_close( $request_socket );
		unset( $this->requests_started[ $request_id ] );
		unset( $this->requests_sockets[ $request_id ] );
		unset( $this->requests_accepted[ $request_id ] );
	}

	public function read_request_header( $request_event, $request_id ) {
		$this->log_method_call( __FUNCTION__ );
		$header = event_buffer_read( $request_event, 4 );
		if ( $header === 'stat' ) {
			unset( $this->requests_accepted[ $request_id ] );
			$this->requests_working[ $request_id ] = $request_event;
			return $this->respond_to_status_request( $request_event, $request_id );
		}
		$length = current( unpack( 'N', $header ) );
		$this->requests_lengths[ $request_id ] = $length;
		event_buffer_watermark_set( $request_event, EV_READ, $length, $length );
		event_buffer_set_callback( $request_event,
			array( $this, 'queue_request' ),
			null,
			array( $this, 'queue_request_error' ),
			$request_id
		);
		event_buffer_enable( $request_event, EV_READ );
	}

	public function queue_request_error( $request_event, $what, $request_id ) {
		$this->log_method_call( __FUNCTION__ );
		event_buffer_disable( $request_event, EV_READ | EV_WRITE );
		event_buffer_free( $request_event );
		$request_socket = $this->requests_sockets[ $request_id ];
		socket_shutdown( $request_socket );
		socket_close( $request_socket );
		unset( $this->requests_started[ $request_id ] );
		unset( $this->requests_sockets[ $request_id ] );
		unset( $this->requests_accepted[ $request_id ] );
	}

	public function queue_request( $request_event, $request_id ) {
		$this->log_method_call( __FUNCTION__ );
		event_buffer_disable( $request_event, EV_READ | EV_WRITE );
		$this->requests_buffered[ $request_id ] = $request_event;
		unset( $this->requests_accepted[ $request_id ] );
		$this->dispatch_request();
	}

	public function accept_offer() {
		$this->log_method_call( __FUNCTION__ );
		$start_time = microtime( true );
		$offer_socket = socket_accept( $this->offer_socket );
		$offer_id = (string) intval( $offer_socket );
		$this->offers_sockets[ $offer_id ] = $offer_socket;
		// Buffer the 4-byte packed PID message
		$event = event_buffer_new( $offer_socket,
			array( $this, 'read_offer' ),
			null,
			array( $this, 'read_offer_error' ),
			$offer_id
		);
		event_buffer_watermark_set( $event, EV_READ, 4, 4 );
		event_buffer_base_set( $event, $this->event_base );
		event_buffer_timeout_set( $event, 1, 1 );
		event_buffer_enable( $event, EV_READ );
		$this->offers_accepted[ $offer_id ] = $event;
	}

	public function read_offer_error( $offer_event, $what, $offer_id ) {
		$this->log_method_call( __FUNCTION__ );
		$offer_socket = $this->offers_sockets[ $offer_id ];
		event_buffer_disable( $offer_event, EV_READ | EV_WRITE );
		event_buffer_free( $offer_event );
		socket_shutdown( $offer_socket );
		socket_close( $offer_socket );
		unset( $this->offers_sockets[ $offer_id ] );
		unset( $this->offers_accepted[ $offer_id ] );
	}

	public function read_offer( $offer_event, $offer_id ) {
		$this->log_method_call( __FUNCTION__ );
		event_buffer_disable( $offer_event, EV_READ | EV_WRITE );
		unset( $this->offers_accepted[ $offer_id ] );
		$header = event_buffer_read( $offer_event, 4 );
		$worker_pid = current( unpack( 'N', $header ) );
		if ( ! $this->recognize_worker( $worker_pid ) ) {
			event_buffer_free( $offer_event );
			$offer_socket = $this->offers_sockets[ $offer_id ];
			unset( $this->offers_sockets[ $offer_id ] );
			socket_shutdown( $offer_socket );
			socket_close( $offer_socket );
			return;
		}
		$this->workers_ready[ $worker_pid ] = $offer_id;
		$this->offers_workers[ $offer_id ] = $worker_pid;
		$this->offers_waiting[ $offer_id ] = $offer_event;
		event_buffer_set_callback( $offer_event,
			null,
			null,
			array( $this, 'ready_worker_error' ),
			$offer_id
		);
		event_buffer_watermark_set( $offer_event, EV_WRITE, 4, 4 );
		event_buffer_enable( $offer_event, EV_WRITE );
		$this->dispatch_request();
	}

	private function recognize_worker( $worker_pid ) {
		if ( isset( $this->workers_starting[ $worker_pid ] ) ) {
			// This is the first offer from this worker
			unset( $this->workers_starting[ $worker_pid ] );
			// Retire one obsolete worker from the ready list
			foreach ( $this->workers_obsolete as $old_pid => $time ) {
				if ( $old_pid == $worker_pid )
					continue;
				if ( isset( $this->workers_ready[ $old_pid ] ) ) {
					$this->retire_worker( $old_pid );
					break;
				}
			}
		}
		if ( isset( $this->workers_obsolete[ $worker_pid ] ) ) {
			$this->retire_worker( $worker_pid );
			return false;
		}
		return isset( $this->workers_alive[ $worker_pid ] );
	}

	public function ready_worker_error( $event, $what, $worker_id ) {
		$this->log_method_call( __FUNCTION__ );
		event_buffer_disable( $event, EV_READ | EV_WRITE );
		event_buffer_free( $event );
		$socket = $this->offers_sockets[ $offer_id ];
		socket_shutdown( $socket );
		socket_close( $socket );
		unset( $this->workers_ready[ $worker_pid ] );
		unset( $this->offers_sockets[ $offer_id ] );
		unset( $this->offers_workers[ $offer_id ] );
		unset( $this->offers_waiting[ $offer_id ] );
	}

	private function dispatch_request() {
		if ( ! $this->requests_buffered )
			return;
		if ( ! $this->workers_ready )
			return;
		// Read a buffered request
		list( $request_id, $request_event ) = $this->take_first( $this->requests_buffered );
		event_buffer_set_callback( $request_event,
			null,
			null,
			array( $this, 'working_request_error' ),
			$request_id
		);
		$request_length = $this->requests_lengths[ $request_id ];
		unset( $this->requests_lengths[ $request_id ] );
		$request_message = event_buffer_read( $request_event, $request_length );
		if ( strlen( $request_message ) != $request_length ) {
			error_log( "Service detected malformed request" );
			$this->close_request( $request_event, $request_id );
			return;
		}
		// Write request to ready worker
		list( $worker_pid, $offer_id ) = $this->take_first( $this->workers_ready );
		$offer_event = $this->offers_waiting[ $offer_id ];
		unset( $this->offers_waiting[ $offer_id ] );
		event_buffer_set_callback( $offer_event,
			null,
			array( $this, 'write_request_success' ),
			array( $this, 'write_request_error' ),
			$offer_id
		);
		$headers = pack( 'N', $request_id ) . pack( 'N', $request_length );
		event_buffer_write( $offer_event, $headers . $request_message );
		$this->offers_written[ $offer_id ] = $offer_event;
		$this->offers_requests[ $offer_id ] = $request_id;
		$this->requests_written[ $request_id ] = $request_event;
		$this->requests_assigned[ $request_id ] = $worker_pid;
	}

	public function write_request_error( $offer_event, $what, $offer_id ) {
		$this->log_method_call( __FUNCTION__ );
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
		$this->close_request( $event, $request_id );
		if ( is_callable( $this->write_request_error_callback ) )
			call_user_func( $this->write_request_error_callback );
	}

	public function write_request_success( $offer_event, $offer_id ) {
		$this->log_method_call( __FUNCTION__ );
		event_buffer_disable( $offer_event, EV_READ | EV_WRITE );
		event_buffer_free( $offer_event );
		$offer_socket = $this->offers_sockets[ $offer_id ];
		socket_shutdown( $offer_socket );
		socket_close( $offer_socket );
		$worker_pid = $this->offers_workers[ $offer_id ];
		$request_id = $this->offers_requests[ $offer_id ];
		$request_event = $this->requests_written[ $request_id ];
		unset( $this->offers_workers[ $offer_id ] );
		unset( $this->requests_written[ $request_id ] );
		unset( $this->workers_ready[ $worker_pid ] );
		unset( $this->offers_sockets[ $offer_id ] );
		unset( $this->offers_written[ $offer_id ] );
		unset( $this->offers_requests[ $offer_id ] );
		$this->requests_working[ $request_id ] = $request_event;
	}

	public function accept_response() {
		$this->log_method_call( __FUNCTION__ );
		$response_socket = socket_accept( $this->response_socket );
		$response_id = (string) intval( $response_socket );
		$this->responses_sockets[ $response_id ] = $response_socket;
		// Buffer the request_id and message_length, 4 bytes each
		$event = event_buffer_new( $response_socket,
			array( $this, 'read_response_headers' ),
			null,
			array( $this, 'read_response_headers_error' ),
			$response_id
		);
		event_buffer_watermark_set( $event, EV_READ, 8, 8 );
		event_buffer_base_set( $event, $this->event_base );
		event_buffer_timeout_set( $event, 1, 1 );
		event_buffer_enable( $event, EV_READ );
		$this->responses_accepted[ $response_id ] = $event;
	}

	public function read_response_headers_error( $response_event, $what, $response_id ) {
		$this->log_method_call( __FUNCTION__ );
		$this->close_response( $response_event, $response_id );
	}

	private function close_response( $response_event, $response_id ) {
		event_buffer_disable( $response_event, EV_READ | EV_WRITE );
		event_buffer_free( $response_event );
		$response_socket = $this->responses_sockets[ $response_id ];
		socket_shutdown( $response_socket );
		socket_close( $response_socket );
		unset( $this->responses_sockets[ $response_id ] );
		unset( $this->responses_lengths[ $response_id ] );
		unset( $this->responses_requests[ $response_id ] );
		unset( $this->responses_accepted[ $response_id ] );
		unset( $this->responses_buffering[ $response_id ] );
	}

	public function read_response_headers( $response_event, $response_id ) {
		$this->log_method_call( __FUNCTION__ );
		event_buffer_disable( $response_event, EV_READ | EV_WRITE );
		$header = event_buffer_read( $response_event, 4 );
		$request_id = current( unpack( 'N', $header ) );
		if ( ! isset( $this->requests_working[ $request_id ] ) ) {
			$this->close_response( $response_event, $response_id );
			return;
		}
		$this->responses_requests[ $response_id ] = $request_id;
		$header = event_buffer_read( $response_event, 4 );
		$length = current( unpack( 'N', $header ) );
		$this->responses_lengths[ $response_id ] = $length;
		event_buffer_watermark_set( $response_event, EV_READ, $length, $length );
		event_buffer_set_callback( $response_event,
			array( $this, 'read_response' ),
			null,
			array( $this, 'read_response_error' ),
			$response_id
		);
		event_buffer_enable( $response_event, EV_READ );
	}

	public function read_response_error( $response_event, $what, $response_id ) {
		$this->log_method_call( __FUNCTION__ );
		$request_id = $this->responses_requests[ $response_id ];
		$this->close_response( $response_event, $response_id );
		$response = $this->create_error_response();
		$response_message = serialize( $response );
		$this->return_response( $request_id, $response_message );
	}

	public function read_response( $response_event, $response_id ) {
		$this->log_method_call( __FUNCTION__ );
		$length = $this->responses_lengths[ $response_id ];
		$response_message = event_buffer_read( $response_event, $length );
		$request_id = $this->responses_requests[ $response_id ];
		$this->close_response( $response_event, $response_id );
		$this->return_response( $request_id, $response_message );
	}

	private function return_response( $request_id, $response_message ) {
		if ( array_key_exists( $request_id, $this->requests_working ) ) {
			$length = strlen( $response_message );
			$event = $this->requests_working[ $request_id ];
			event_buffer_disable( $event, EV_READ | EV_WRITE );
			event_buffer_watermark_set( $event, EV_WRITE, 0, 0 );
			event_buffer_timeout_set( $event, 1, 1 );
			event_buffer_set_callback( $event,
				null,
				array( $this, 'close_request' ),
				array( $this, 'return_response_error' ),
				$request_id
			);
			event_buffer_enable( $event, EV_WRITE );
			$header = pack( 'N', $length );
			event_buffer_write( $event, $header . $response_message );
			unset( $this->requests_assigned[ $request_id ] );
		}
	}

	public function return_response_error( $event, $what, $request_id ) {
		$this->log_method_call( __FUNCTION__ );
		$this->close_request( $event, $request_id );
	}

	public function working_request_error( $event, $what, $request_id ) {
		$this->log_method_call( __FUNCTION__ );
		$this->close_request( $event, $request_id );
	}

	public function close_request( $event, $request_id ) {
		$this->log_method_call( __FUNCTION__ );
		event_buffer_disable( $event, EV_READ | EV_WRITE );
		event_buffer_free( $event );
		$request_socket = $this->requests_sockets[ $request_id ];
		socket_shutdown( $request_socket );
		socket_close( $request_socket );
		unset( $this->requests_working[ $request_id ] );
		unset( $this->requests_started[ $request_id ] );
		unset( $this->requests_sockets[ $request_id ] );
		unset( $this->requests_assigned[ $request_id ] );
	}

	public function SIGCHLD() {
		$this->log_method_call( __FUNCTION__ );
		// Reap zombies until none remain
		while ( true ) {
			$pid = pcntl_wait( $status, WNOHANG );
			if ( $pid < 1 )
				break;
			$this->remove_worker( $pid );
		}
		$this->supervise_workers();
	}

	public function SIGHUP() {
		$this->log_method_call( __FUNCTION__ );
		// Reload a limited list of options from ini file
		$this->load_ini_file( $this->ini_options_HUP );
		// Make old workers obsolete
		$this->workers_obsolete = $this->workers_alive;
		// Spawn replacements
		for ( $i = $this->max_workers - count( $this->workers_starting ); $i > 0; --$i ) {
			if ( $this->become_worker() )
				return;
		}
	}

	public function SIGINT() {
		$this->log_method_call( __FUNCTION__ );
		$this->received_SIGINT = true;
		error_log( "Prefork caught SIGINT. Requests accepted: "
			. count( $this->requests_accepted )
			. " working: "
			. count( $this->requests_working ) );
		if ( $this->service_shutdown )
			return $this->continue_shutdown();
		$this->begin_shutdown();
	}

	public function SIGTERM() {
		$this->log_method_call( __FUNCTION__ );
		$this->received_SIGTERM = true;
		error_log( "Prefork caught SIGTERM. Requests accepted: "
			. count( $this->requests_accepted )
			. " working: "
			. count( $this->requests_working ) );
		if ( $this->service_shutdown )
			return $this->continue_shutdown();
		$this->begin_shutdown();
	}

	private function retire_worker( $pid ) {
		$this->log_method_call( __FUNCTION__ );
		// Do we have a socket to the worker?
		if ( isset( $this->workers_ready[ $pid ] ) ) {
			$offer = $this->workers_ready[ $pid ];
			$socket = $this->offers_sockets[ $offer ];
			try {
				@socket_send( $socket, 'BYE!', 4, 0 );
			} catch ( Exception $e ) { }
		}
		$this->remove_worker( $pid );
	}

	private function remove_worker( $pid ) {
		$this->log_method_call( __FUNCTION__ );
		if ( isset( $this->workers_ready[ $pid ] ) ) {
			$offer_id = $this->workers_ready[ $pid ];
			$socket = $this->offers_sockets[ $offer_id ];
			@socket_shutdown( $socket );
			@socket_close( $socket );
			unset( $this->offers_sockets[ $offer_id ] );
			unset( $this->offers_waiting[ $offer_id ] );
			unset( $this->offers_workers[ $offer_id ] );
		}
		unset( $this->workers_ready[ $pid ] );
		unset( $this->workers_obsolete[ $pid ] );
		unset( $this->workers_starting[ $pid ] );
		unset( $this->workers_alive[ $pid ] );
	}

	private function supervise_workers() {
		// Spawn workers to fill empty slots
		while ( count( $this->workers_alive ) < $this->max_workers ) {
			if ( $this->become_worker() )
				break;
		}
	}

	private function supervise_requests() {
		if ( empty( $this->requests_started ) )
			return;
		// Close any requests that have timed out
		reset( $this->requests_started );
		list( $request_id, $time ) = each( $this->requests_started );
		$timeout = $time + $this->gateway_timeout;
		if ( $timeout < microtime( true ) ) {
			$this->log_method_call( __FUNCTION__ );
			$request_event = $this->requests_working[$request_id];
			$this->close_request( $request_event, $request_id );
			$this->supervise_requests();
		}
	}

	private function become_worker() {
		$this->log_method_call( __FUNCTION__ );
		$pid = $this->fork_process();
		if ( $pid === 0 ) {
			$this->worker_pid = posix_getpid();
			// The child process ignores SIGINT (small race here)
			pcntl_sigprocmask( SIG_BLOCK, array(SIGINT) );
			// and lets go of the parent's libevent base
			event_base_reinit( $this->event_base );
			// and breaks out of the service event loop
			event_base_loopbreak( $this->event_base );
			// and dismantles the whole event structure
			foreach ( $this->events as $i => $event ) {
				event_del( $event );
				event_free( $event );
				unset( $this->events[$i] );
			}
			$bufferevents = array_merge(
				$this->offers_accepted,
				$this->offers_waiting,
				$this->offers_written,
				$this->requests_accepted,
				$this->requests_buffered,
				$this->requests_written,
				$this->requests_working,
				$this->responses_accepted,
				$this->responses_buffering
			);
			foreach ( $bufferevents as $event ) {
				event_buffer_disable( $event, EV_READ | EV_WRITE );
				event_buffer_free( $event );
			}
			event_base_free( $this->event_base );
			unset( $this->event_base );
			$this->worker = new Prefork_Worker( $this );
			return true;
		}
		$start_time = microtime( true );
		$this->workers_alive[ $pid ] = $start_time;
		$this->workers_starting[ $pid ] = $start_time;
		return false;
	}

	private function begin_shutdown() {
		$this->log_method_call( __FUNCTION__ );
		error_log( "Prefork service shutdown intiated" );
		// Stop listening for events on the request port
		event_del( $this->events['request'] );
		// Release the port so the next service can bind it
		socket_shutdown( $this->request_socket, 0 );
		$this->service_shutdown = true;
	}

	private function continue_shutdown() {
		$this->log_method_call( __FUNCTION__ );
		// Delay shutdown until all accepted requests are completed
		if ( $this->requests_started )
			return;
		event_base_loopbreak( $this->event_base );
		foreach ( $this->workers_alive as $pid => $time )
			posix_kill( $pid, SIGKILL );
		$this->unlink_pidfile();
		error_log( "Prefork service shutdown complete" );
		if ( $this->received_SIGINT ) {
			pcntl_signal( SIGINT, SIG_DFL );
			posix_kill( posix_getpid(), SIGINT );
			// The following unreachable line is merely informative
			exit(130);
		}
		if ( $this->received_SIGTERM ) {
			pcntl_signal( SIGTERM, SIG_DFL );
			posix_kill( posix_getpid(), SIGTERM );
			// The following unreachable line is merely informative
			exit(143);
		}
		exit(0);
	}

	private function respond_to_status_request( $request_event, $request_id ) {
		$this->log_method_call( __FUNCTION__ );
		$response = $this->create_status_response();
		$response_message = serialize( $response );
		$this->return_response( $request_id, $response_message );
	}

	private function create_status_response() {
		$report = '';
		foreach ( get_class_vars( __CLASS__ ) as $var => $default ) {
			if ( $default === array() && $var != 'method_calls' ) {
				$count = count( $this->{$var} );
				// Scrub the current status request from the counts
				if ( $var == 'requests_started' || $var == 'requests_sockets' || $var == 'requests_working' )
					--$count;
				$report .= "$var: $count" . PHP_EOL;
			}
		}
		foreach ( $this->method_calls as $method => $calls ) {
			$report .= __CLASS__ . "::$method(): $calls" . PHP_EOL;
		}
		return array(
			'code' => 200,
			'headers' => array(
				'Content-Type: text/plain',
				'X-Prefork-Status: OK',
			),
			'body' => $report,
		);
	}

	private function create_sockets() {
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

	/***** Internal methods *****/

	protected function event_add( $name, $fd, $flags, $callback, $timeout = -1 ) {
		$event = event_new();
		event_set( $event, $fd, $flags, array( $this, $callback ) );
		event_base_set( $event, $this->event_base );
		event_add( $event, $timeout );
		$this->events[ $name ] = $event;
	}

	private function take_first( &$array ) {
		reset( $array );
		list( $key, $value ) = each( $array );
		unset( $array[ $key ] );
		return array( $key, $value );
	}

	private function log_method_call( $function ) {
		if ( ! isset( $this->method_calls[ $function ] ) )
			$this->method_calls[ $function ] = 0;
		++$this->method_calls[ $function ];
	}
}

class Prefork_Worker extends Prefork_Service {
	protected $ini_options = array(
		'worker_pid',
		'offer_address',
		'offer_port',
		'response_address',
		'response_port',
		'min_free_ram',
		'single_interns',
		'prefork_callback',
		'postfork_callback',
	);

	public $request_id;
	public $intern;
	private $intern_pid;

	private function become_intern( $request_message ) {
		$intern = new Prefork_Intern( $this );
		$pid = $this->fork_process();
		if ( $pid === 0 ) {
			// Interns only past this point
			event_base_loopbreak( $this->event_base );
			event_base_reinit( $this->event_base );
			foreach ( $this->events as $i => $event ) {
				event_del( $event );
				event_free( $event );
				unset( $this->events[$i] );
			}
			event_base_free( $this->event_base );
			$intern->start_request( $request_message );
			$this->intern = $intern;
			return true;
		}
		$this->intern_pid = $pid;
		if ( ! $this->single_interns )
			$this->make_offer();
		return false;
	}

	public function fork() {
		// Check requirements
		if ( ob_get_level() > 1 )
			die( 'Prefork Error: other output buffers already started in application loader.' );
		if ( is_callable( $this->prefork_callback ) )
			call_user_func( $this->prefork_callback );
		// The worker stays in this loop, spawning slaves
		$this->event_loop();
	}

	private function event_loop() {
		$this->event_base = event_base_new();
		$this->event_add( 'SIGCHLD', SIGCHLD, EV_SIGNAL | EV_PERSIST,
			'SIGCHLD' );
		$this->make_offer();
		event_base_loop( $this->event_base );
	}

	public function SIGCHLD() {
		// Reap zombies
		while ( true ) {
			$pid = pcntl_wait( $status, WNOHANG );
			if ( $pid < 1 )
				break;
			if ( ! pcntl_wifexited( $status ) ) {
				// Intern exited abnormally
				error_log( "Worker $this->worker_pid caught abnormal exit by intern $pid" );
				// TODO: send error response
			}
			if ( $this->single_interns )
				$this->make_offer();
		}
	}

	private function make_offer() {
		$this->offer_socket = socket_create( AF_INET, SOCK_STREAM, 0 );
		//socket_set_nonblock( $this->offer_socket ); // Maybe connect async (change callbacks, add write_offer)
		socket_connect( $this->offer_socket, $this->offer_address, $this->offer_port );
		$this->offer_event = event_buffer_new( $this->offer_socket,
			null,
			array( $this, 'write_offer_success' ),
			array( $this, 'write_offer_error' ),
			null
		);
		event_buffer_watermark_set( $this->offer_event, EV_WRITE, 0, 0 );
		event_buffer_base_set( $this->offer_event, $this->event_base );
		event_buffer_enable( $this->offer_event, EV_WRITE );
		$offer = pack( 'N', $this->worker_pid );
		event_buffer_write( $this->offer_event, $offer );
	}

	public function write_offer_success() {
		event_buffer_disable( $this->offer_event, EV_WRITE );
		event_buffer_watermark_set( $this->offer_event, EV_READ, 4, 4 );
		event_buffer_set_callback( $this->offer_event,
			array( $this, 'read_request_id' ),
			null,
			array( $this, 'read_request_id_error' ),
			null
		);
		event_buffer_enable( $this->offer_event, EV_READ );
	}

	public function write_offer_error() {
		error_log( "Worker $this->worker_pid reached write_offer_error" );
		exit(0);
	}

	public function read_request_id() {
		$message = event_buffer_read( $this->offer_event, 4 );
		if ( $message === 'BYE!' )
			$this->retire();
		$this->request_id = current( unpack( 'N', $message ) );
		event_buffer_watermark_set( $this->offer_event, EV_READ, 4, 4 );
		event_buffer_set_callback( $this->offer_event,
			array( $this, 'read_request_header' ),
			null,
			array( $this, 'read_request_header_error' ),
			$length
		);
		event_buffer_enable( $this->offer_event, EV_READ );
	}

	public function read_request_id_error() {
		error_log( "Worker $this->worker_pid reached read_request_id_error" );
		exit(0);
	}

	public function read_request_header() {
		$header = event_buffer_read( $this->offer_event, 4 );
		$length = current( unpack( 'N', $header ) );
		if ( $length > $this->max_message_length ) {
			error_log( "Worker $this->worker_pid received request length $length, aborting" );
			exit(0);
		}
		event_buffer_watermark_set( $this->offer_event, EV_READ, $length, $length );
		event_buffer_set_callback( $this->offer_event,
			array( $this, 'read_request' ),
			null,
			array( $this, 'read_request_error' ),
			$length
		);
		event_buffer_enable( $this->offer_event, EV_READ );
	}

	public function read_request_header_error() {
		error_log( "Worker $this->worker_pid reached read_request_header_error" );
		exit(0);
	}

	public function read_request( $event, $length ) {
		$request_message = event_buffer_read( $this->offer_event, $length );
		event_buffer_disable( $this->offer_event, EV_READ | EV_WRITE );
		event_buffer_free( $this->offer_event );
		unset( $this->offer_event );
		socket_shutdown( $this->offer_socket );
		socket_close( $this->offer_socket );
		unset( $this->offer_socket );
		$this->become_intern( $request_message );
	}

	public function read_request_error() {
		error_log( "Worker $this->worker_pid reached read_request_error" );
		exit(0);
	}

	private function wait_for_resources( $interval = 10000 ) {
		while ( ! $this->has_free_resources() )
			usleep( $interval );
	}

	private function has_free_resources() {
		if ( ! $this->min_free_ram )
			return true;
		$meminfo = file_get_contents( '/proc/meminfo' ); // Linux
		preg_match( '/^MemTotal:\s+(\d+)/m', $meminfo, $matches );
		$total = $matches[1];
		preg_match( '/^MemFree:\s+(\d+)/m', $meminfo, $matches );
		$free = $matches[1];
		preg_match( '/^Buffers:\s+(\d+)/m', $meminfo, $matches );
		$buffers = $matches[1];
		preg_match( '/^Cached:\s+(\d+)/m', $meminfo, $matches );
		$cached = $matches[1];
		$free_ram = $free + $buffers + $cached;
		return ( $free_ram / $total > $this->min_free_ram );
	}

	private function retire() {
		if ( is_callable( $this->retire_callback ) )
			call_user_func( $this->retire_callback );
		exit(0);
	}

	protected function send_response_to_service( $response ) {
		$socket = socket_create( AF_INET, SOCK_STREAM, 0 );
		socket_connect( $socket, $this->response_address, $this->response_port );
		$header = pack( 'N', $this->request_id );
		socket_send( $socket, $header, 4, 0 );
		$response_message = serialize( $response );
		$this->write_message( $socket, $response_message );
		socket_shutdown( $socket );
		socket_close( $socket );
	}
}

class Prefork_Intern extends Prefork_Worker {
	protected $ini_options = array(
		'response_address',
		'response_port',
		'request_id',
		'postfork_callback',
	);

	protected function start_request( $request ) {
		// Prepare request variables
		$request = unserialize( $request );
		if ( isset( $request['SERVER'] ) )  $_SERVER  = $request['SERVER'];
		if ( isset( $request['GET'] ) )     $_GET     = $request['GET'];
		if ( isset( $request['POST'] ) )    $_POST    = $request['POST'];
		if ( isset( $request['COOKIE'] ) )  $_COOKIE  = $request['COOKIE'];
		if ( isset( $request['REQUEST'] ) ) $_REQUEST = $request['REQUEST'];
		if ( isset( $request['FILES'] ) )   $_FILES   = $request['FILES'];
		if ( isset( $request['SESSION'] ) ) $_SESSION = $request['SESSION'];
		if ( isset( $request['ENV'] ) )     $_ENV     = $request['ENV'];
		if ( isset( $request['HRPD'] ) )    $GLOBALS['HTTP_RAW_POST_DATA'] = $request['HRPD'];
		if ( !empty( $_POST ) && empty( $HTTP_RAW_POST_DATA ) )
			$GLOBALS['HTTP_RAW_POST_DATA'] = http_build_query( $_POST );
		// Prepare to collect output
		ob_start( array( $this, 'ob_handler' ) );
		// Signal the app that we are starting a request
		if ( is_callable( $this->postfork_callback ) )
			call_user_func( $this->postfork_callback );
	}

	public function ob_handler( $output ) {
		$response = array(
			'code'    => http_response_code(),
			'headers' => headers_list(),
			'body'    => $output,
		);
		$this->send_response_to_service( $response );
		return '';
	}

	public function finish_request() {
		// Trigger ob_handler() above
		while ( @ob_end_flush() );
	}
}
