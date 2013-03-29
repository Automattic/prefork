<?php

class Prefork {
	// Configuration
	public $prefork_callback;
	public $postfork_callback;
	public $max_workers = 8;

	// Child process identifiers
	private $worker_pids = array();
	private $workers_needing_restart = array();

	private $continue = true;

	public function __construct( $transport = 'Sockets' ) {
		if ( version_compare( PHP_VERSION, '5.4', '<' ) )
			die( 'Error: Prefork requires PHP 5.4 for http_response_code().' . PHP_EOL );

		$transport_class = 'Prefork_Transport_' . $transport;
		if ( ! in_array( 'Prefork_Transport', class_implements( $transport_class ) ) )
			die( "Error: class $transport_class does not implement Prefork_Transport." );
		$this->transport = new $transport_class;
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

		$response = $this->transport->agent__transact_with_service( $request );

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

	public function become_service() {
		if ( substr( php_sapi_name(), 0, 3 ) != 'cgi' )
			die( 'Error: Prefork service used with unsupported SAPI. PHP was invoked with "' . php_sapi_name() . '" SAPI. The Prefork Service requires a "cgi" SAPI to capture headers. Try executing "php-cgi" at the command line.' . PHP_EOL );

		$this->transport->service__start();

		// Install our signal handler
		declare ( ticks = 1 ); // Catch signals quickly
		pcntl_signal( SIGINT, array( $this, 'sig_handler' ) );

		$this->service_loop();

		// Restore default signal handler
		pcntl_signal( SIGINT, SIG_DFL );
	}

	public function sig_handler( $signal ) {
		if ( $signal == SIGINT ) {
			print 'Service received SIGINT. Shutting down.' . PHP_EOL;
			$this->service__shutdown();
		}
		if ( $signal == SIGHUP ) {
			print 'Service recieved SIGHUP. Restarting workers.' . PHP_EOL;
			$this->workers_needing_restart = $this->worker_pids;
		}
	}

	private function service_loop() {
		while ( $this->service__continue() ) {
			$this->service__reap_dead_workers();
			while ( $this->service__needs_more_workers() )
				if ( $this->service__become_worker() )
					return; // Child process loads app
			$this->transport->service__shuttle_responses_until_a_worker_is_ready();
			$this->transport->service__shuttle_responses_until_a_request_is_ready();
			$this->transport->service__shuttle_request_to_worker();
		}
		$this->service__shutdown();
	}

	private function service__continue() {
		if ( ! $this->continue )
			return false;
		if ( ! $this->transport->service__continue() )
			return false;
		return true;
	}

	private function service__reap_dead_workers() {
		do {
			// Non-blocking check for child exit status
			$pid = pcntl_wait( $status, WNOHANG );
			// Return of zero means no zombie children detected
			if ( $pid === 0 )
				return;
			if ( $pid > 0 )
				unset( $this->worker_pids[ $pid ] );
		} while ( $pid > 0 );
	}

	private function service__needs_more_workers() {
		return count( $this->worker_pids ) < $this->max_workers;
	}

	private function service__become_worker() {
		$pid = $this->fork_process();
		if ( $pid === 0 ) {
			$this->transport->service__become_worker();
			return true;
		}
		$this->worker_pids[ $pid ] = microtime();
		return false;
	}

	private function worker__become_intern() {
		$pid = $this->fork_process();
		if ( $pid === 0 ) {
			$this->transport->worker__become_intern();
			return true;
		}
		return false;
	}

	private function fork_process() {
		$pid = pcntl_fork();
		if ( $pid === -1 )
			die( "Fork failure\n" );
		return $pid;
	}

	public function service__create_error_response( $request, $status ) {
		return array(
			'code' => 500,
			'headers' => array(),
			'body' => 'Internal server error',
		);
	}

	public function service__shutdown() {
		print "Service shutting down" . PHP_EOL;
		$this->transport->service__shutdown();
		exit;
	}

	public function fork() {
		// Check requirements
		if ( ob_get_level() > 1 )
			die( 'Prefork Error: other output buffers already started in application loader.' );

		if ( is_callable( $this->prefork_callback ) )
			call_user_func( $this->prefork_callback );

		// The worker stays in this loop, spawning slaves
		while ( true ) {
			$request = $this->transport->worker__receive_request();
			if ( $request === 'KILL' )
				exit;

			if ( $this->worker__become_intern() )
				break; // Child process assumes request vars, runs app & sends response

			// Block until the child exits
			pcntl_waitpid( $pid, $status );
			if ( $status > 0 ) {
				$response = $this->service__create_error_response( $request, $status );
				$this->transport->worker__send_response_to_service( $response );
			}
		}

		if ( is_callable( $this->postfork_callback ) )
			call_user_func( $this->postfork_callback );

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

		// Prepare to collect output
		ob_start( array( $this, 'intern__ob_handler' ) );
	}

	public function intern__ob_handler( $output ) {
		$response = array(
			'code'    => http_response_code(),
			'headers' => headers_list(),
			'body'    => $output,
		);

		$this->transport->intern__send_response_to_service( $response );

		return '';
	}
}

interface Prefork_Transport {
	function agent__transact_with_service( $request ); // return $response;
	function service__start();
	function service__continue(); // return bool
	function service__poll_for_responses();
	function service__poll_for_responses_and_workers();
	function service__poll_for_responses_and_requests();
	function service__shuttle_responses_until_a_worker_is_ready();
	function service__shuttle_responses_until_a_request_is_ready();
	function service__shuttle_request_to_worker();
	function service__become_worker();
	function service__shutdown();
	function worker__receive_request(); // return $request;
	function worker__send_response_to_service( $response );
	function worker__become_intern();
	function intern__send_response_to_service( $response );
}

class Prefork_Transport_Sockets implements Prefork_Transport {
	public $frontend_address = "/tmp/prefork-frontend";
	public $frontend_backlog = 64;
	public $backend_request_address = "/tmp/prefork-backend-request";
	public $backend_request_backlog = 64;
	public $backend_response_address = "/tmp/prefork-backend-response";
	public $backend_response_backlog = 64;

	// Sockets for listening
	private $frontend_socket;
	private $backend_request_socket;
	private $backend_response_socket;

	// Sockets for accepted connections
	private $ready_worker_socket;
	private $ready_request_socket;
	private $ready_response_socket;

	private $pending_request_sockets = array();

	private $return_address;

	public function __construct() {
	}

	/***** Interface methods *****/

	public function agent__transact_with_service( $request ) {
		$socket = socket_create( AF_UNIX, SOCK_STREAM, 0 );
		socket_connect( $socket, $this->frontend_address );
		$request_message = serialize( $request );
		$this->write_to_socket( $socket, $request_message );
		$response_message = $this->read_from_socket( $socket );
		$response = unserialize( $response_message );
		return $response;
	}

	public function service__start() {
		// Prepare to accept connections from Agents
		$this->frontend_socket = socket_create( AF_UNIX, SOCK_STREAM, 0 );
		socket_set_option($this->frontend_socket, SOL_SOCKET, SO_REUSEADDR, 1);
		if ( ! socket_bind( $this->frontend_socket, $this->frontend_address ) )
			die( socket_strerror( socket_last_error() ) . PHP_EOL );
		socket_listen( $this->frontend_socket, $this->frontend_backlog );

		// Prepare to accept connections from Workers ready for requests
		$this->backend_request_socket = socket_create( AF_UNIX, SOCK_STREAM, 0 );
		socket_set_option($this->backend_request_socket, SOL_SOCKET, SO_REUSEADDR, 1);
		socket_bind( $this->backend_request_socket, $this->backend_request_address );
		socket_listen( $this->backend_request_socket, $this->backend_request_backlog );

		// Prepare to accept connections from Workers ready with responses
		$this->backend_response_socket = socket_create( AF_UNIX, SOCK_STREAM, 0 );
		socket_set_option($this->backend_response_socket, SOL_SOCKET, SO_REUSEADDR, 1);
		socket_bind( $this->backend_response_socket, $this->backend_response_address );
		socket_listen( $this->backend_response_socket, $this->backend_response_backlog );
	}

	public function service__continue() {
		return true;
	}

	public function service__poll_for_responses() {
		$this->ready_response_socket = false;
		$read = array( $this->backend_response_socket );
		$write = $except = array();
		socket_select( $read, $write, $except, null );
		if ( in_array( $this->backend_response_socket, $read, true ) )
			$this->ready_response_socket = socket_accept( $this->backend_response_socket );
	}

	public function service__poll_for_responses_and_workers() {
		$this->ready_response_socket = false;
		$this->ready_worker_socket = false;
		$read = array( $this->backend_response_socket, $this->backend_request_socket );
		$write = $except = array();
		socket_select( $read, $write, $except, null );
		if ( in_array( $this->backend_response_socket, $read, true ) )
			$this->ready_response_socket = socket_accept( $this->backend_response_socket );
		if ( in_array( $this->backend_request_socket, $read, true ) )
			$this->ready_worker_socket = socket_accept( $this->backend_request_socket );
	}

	public function service__poll_for_responses_and_requests() {
		$this->ready_response_socket = false;
		$this->ready_request_socket = false;
		$read = array( $this->backend_response_socket, $this->frontend_socket );
		$write = $except = array();
		socket_select( $read, $write, $except, null );
		if ( in_array( $this->backend_response_socket, $read, true ) )
			$this->ready_response_socket = socket_accept( $this->backend_response_socket );
		if ( in_array( $this->frontend_socket, $read, true ) )
			$this->ready_request_socket = socket_accept( $this->frontend_socket );
	}

	public function service__shuttle_responses_until_a_worker_is_ready() {
		do {
			$this->service__poll_for_responses_and_workers();
			if ( $this->ready_response_socket )
				$this->shuttle_response();
		} while ( ! $this->ready_worker_socket );
	}

	public function service__shuttle_responses_until_a_request_is_ready() {
		do {
			$this->service__poll_for_responses_and_requests();
			if ( $this->ready_response_socket )
				$this->shuttle_response();
		} while ( ! $this->ready_request_socket );
	}

	public function service__shuttle_request_to_worker() {
		$return_address = (string) intval( $this->ready_request_socket );
		$this->pending_request_sockets[ $return_address ] = $this->ready_request_socket;
		$request = $this->read_from_socket( $this->ready_request_socket );
		$this->write_to_socket( $this->ready_worker_socket, $return_address );
		$this->write_to_socket( $this->ready_worker_socket, $request );
		socket_close( $this->ready_worker_socket );
	}

	public function service__become_worker() {
	}

	public function service__shutdown() {
		while ( $this->pending_request_sockets ) {
			$this->service__poll_for_responses();
			$this->shuttle_response();
		}
		socket_close( $this->frontend_socket );
		socket_close( $this->backend_request_socket );
		socket_close( $this->backend_response_socket );
		unlink( $this->frontend_address );
		unlink( $this->backend_request_address );
		unlink( $this->backend_response_address );
	}

	public function worker__receive_request() {
		$socket = socket_create( AF_UNIX, SOCK_STREAM, 0 );
		socket_connect( $socket, $this->backend_request_address );
		$this->return_address = $this->read_from_socket( $socket );
		$request_message = $this->read_from_socket( $socket );
		socket_close( $socket );
		$request = unserialize( $request_message );
		return $request;
	}

	public function worker__send_response_to_service( $response ) {
		return intern__send_response_to_service( $response );
	}

	public function worker__become_intern() {
	}

	public function intern__send_response_to_service( $response ) {
		$socket = socket_create( AF_UNIX, SOCK_STREAM, 0 );
		socket_connect( $socket, $this->backend_response_address );
		$this->write_to_socket( $socket, $this->return_address );
		$response_message = serialize( $response );
		$this->write_to_socket( $socket, $response_message );
		socket_close( $socket );
	}

	/***** Internal methods *****/

	private function read_from_socket( $socket ) {
		socket_set_block( $socket );
		socket_recv( $socket, $header, 4, MSG_WAITALL );
		$length = current( unpack( 'N', $header ) );
		socket_recv( $socket, $message, $length, MSG_WAITALL );
		return $message;
	}

	private function write_to_socket( $socket, $message ) {
		socket_set_nonblock( $socket );
		$length = strlen( $message );
		$header = pack( 'N', $length );
		socket_send( $socket, $header, 4, 0 );
		socket_send( $socket, $message, $length, 0 );
	}

	private function shuttle_response() {
		$return_address = $this->read_from_socket( $this->ready_response_socket );
		$response = $this->read_from_socket( $this->ready_response_socket );
		$agent_socket = $this->pending_request_sockets[ $return_address ];
		$this->write_to_socket( $agent_socket, $response );
		unset( $this->pending_request_sockets[ $return_address ] );
		socket_close( $agent_socket );
	}
}

