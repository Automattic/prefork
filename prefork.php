<?php

class Prefork {
	public $prefork_callback;
	public $postfork_callback;

	// Worker config
	public $max_workers = 4;

	// Process identifiers
	private $service_pid;
	private $worker_pids = array();

	public function __construct( $transport = 'ZMQ' ) {
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

		$this->transport->become_service();

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
		exit;
	}

	private function service__continue() {
		return true;
	}

	private function service__reap_dead_workers( $timeout = 0 ) {
		do {
			// Non-blocking check for child exit status
			$pid = pcntl_wait( $status, $timeout ? $timeout : WNOHANG );
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
		$this->transport->service__shutdown();
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
	function become_service();
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

class Prefork_Transport_ZMQ implements Prefork_Transport {
	// 0MQ connections
	public $frontend_bind_interface = "ipc:///tmp/prefork-frontend";
	public $frontend_connect_address = "ipc:///tmp/prefork-frontend";
	public $backend_request_bind_interface = "ipc:///tmp/prefork-backend-request";
	public $backend_request_connect_address = "ipc:///tmp/prefork-backend-request";
	public $backend_response_bind_interface = "ipc:///tmp/prefork-backend-response";
	public $backend_response_connect_address = "ipc:///tmp/prefork-backend-response";
//	public $frontend_bind_interface = "tcp://lo:8082";
//	public $frontend_connect_address = "tcp://localhost:8082";
//	public $backend_bind_interface = "tcp://lo:8083";
//	public $backend_connect_address = "tcp://localhost:8083";

	// 0MQ resources
	private $service_context;
	private $frontend_socket;
	private $backend_request_socket;
	private $backend_response_socket;
	private $worker_request_context;
	private $worker_request_socket;
	private $inbox_poll;
	private $inbox_readable = array();
	private $inbox_writeable = array();
	private $workers_poll;
	private $workers_readable = array();
	private $workers_writeable = array();
	private $return_address;

	public function __construct() {
		if ( ! extension_loaded( 'zmq' ) )
			die( 'Error: ' . __CLASS__ . ' requires the "zmq" extension for PHP.' . PHP_EOL );
	}

	/***** Interface methods *****/

	public function agent__transact_with_service( $request ) {
		$context = new ZMQContext( 1, false );
		$socket = $context->getSocket( ZMQ::SOCKET_REQ );
		$socket->setSockOpt( ZMQ::SOCKOPT_SNDHWM, 0 );
		$socket->connect( $this->frontend_connect_address );
		$this->zmq_send( $socket, $request );
		$response = $this->zmq_recv( $socket );
		return $response;
	}

	public function become_service() {
		// Bind the front and back ends
		$this->service_context = new ZMQContext( 1, false );
		$this->frontend_socket = $this->service_context->getSocket( ZMQ::SOCKET_ROUTER );
		$this->frontend_socket->setSockOpt( ZMQ::SOCKOPT_RCVHWM, 0 );
		$this->frontend_socket->bind( $this->frontend_bind_interface );
		$this->backend_request_socket = $this->service_context->getSocket( ZMQ::SOCKET_PUSH );
		$this->backend_request_socket->setSockOpt( ZMQ::SOCKOPT_SNDHWM, 0 );
		$this->backend_request_socket->bind( $this->backend_request_bind_interface );
		$this->backend_response_socket = $this->service_context->getSocket( ZMQ::SOCKET_PULL );
		$this->backend_response_socket->bind( $this->backend_response_bind_interface );

		// Create a poll for incoming requests and responses
		$this->inbox_poll = new ZMQPoll();
		$this->inbox_poll->add( $this->frontend_socket, ZMQ::POLL_IN );
		$this->inbox_poll->add( $this->backend_response_socket, ZMQ::POLL_IN );

		// Create a poll for workers awaiting requests and sending responses
		$this->workers_poll = new ZMQPoll();
		$this->workers_poll->add( $this->backend_request_socket, ZMQ::POLL_OUT );
		$this->workers_poll->add( $this->backend_response_socket, ZMQ::POLL_IN );
	}

	public function service__shuttle_responses_until_a_worker_is_ready() {
		do {
			$this->poll_workers();
			if ( $this->workers_readable )
				$this->shuttle_response();
		} while ( ! $this->workers_writeable );
	}

	public function service__shuttle_responses_until_a_request_is_ready() {
		do {
			$this->poll_inbox();
			if ( in_array( $this->backend_response_socket, $this->inbox_readable, true ) )
				$this->shuttle_response();
		} while ( ! in_array( $this->frontend_socket, $this->inbox_readable, true ) );
	}

	public function service__shuttle_request_to_worker() {
		$request_envelope = $this->frontend_socket->recvMulti();
		$this->backend_request_socket->sendmulti( $request_envelope );
	}

	public function service__become_worker() {
		$this->worker_context = new ZMQContext( 1, false );
		$this->worker_request_socket = $this->worker_context->getSocket( ZMQ::SOCKET_PULL );
		$this->worker_request_socket->setSockOpt( ZMQ::SOCKOPT_RCVHWM, 0 );
		$this->worker_request_socket->connect( $this->backend_request_connect_address );
		$this->worker_response_socket = $this->worker_context->getSocket( ZMQ::SOCKET_PUSH );
		$this->worker_response_socket->setSockOpt( ZMQ::SOCKOPT_SNDHWM, 0 );
		$this->worker_response_socket->connect( $this->backend_response_connect_address );
	}

	public function service__shutdown() {
		// Unbind immediately to allow 
		$this->frontend_socket->unbind( $this->frontend_bind_interface );
	}

	public function worker__receive_request() {
		$first_message_part = $this->worker_request_socket->recv();
		// A single-part message is a signal
		if ( ! $this->worker_request_socket->getSockOpt( ZMQ::SOCKOPT_RCVMORE ) )
			return $first_message_part;
		// Otherwise the first part is the return address
		$this->return_address = $first_message_part;
		$request = $this->zmq_recv( $this->worker_request_socket );
		return $request;
	}

	public function worker__send_response_to_service( $response ) {
		// The ZMQ::SOCKET_ROUTER needs an address and delimiter
		$this->worker_response_socket->send( $this->return_address, ZMQ::MODE_SNDMORE );
		$this->worker_response_socket->send(                    '', ZMQ::MODE_SNDMORE );
		$result = $this->zmq_send( $this->worker_response_socket, $response );
	}

	public function worker__become_intern() { }

	public function intern__send_response_to_service( $response ) {
		$context = new ZMQContext( 1, false );
		$socket = $context->getSocket( ZMQ::SOCKET_PUSH );
		$socket->connect( $this->backend_response_connect_address );
		// The ZMQ::SOCKET_ROUTER needs an address and delimiter
		$socket->send( $this->return_address, ZMQ::MODE_SNDMORE );
		$socket->send(                    '', ZMQ::MODE_SNDMORE );
		$this->zmq_send( $socket, $response );
	}

	/***** Internal methods *****/

	private function zmq_send( $socket, $message ) {
		return $socket->send( serialize( $message ) );
	}

	private function zmq_recv( $socket ) {
		$message = $socket->recvMulti();
		return unserialize( implode( '', $message ) );
	}

	private function shuttle_response() {
		$response_envelope = $this->backend_response_socket->recvMulti();
		$this->frontend_socket->sendmulti( $response_envelope );
	}

	private function poll_inbox( $block = true ) {
		$timeout = $block ? -1 : 0;
		$this->inbox_poll->poll( $this->inbox_readable, $this->inbox_writeable, $timeout );
	}

	private function poll_workers( $block = true ) {
		$timeout = $block ? -1 : 0;
		$this->workers_poll->poll( $this->workers_readable, $this->workers_writeable, $timeout );
	}
}
