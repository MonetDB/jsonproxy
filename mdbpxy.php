<?php
// create user "phproxy" with password 'phproxy' name 'phproxy' schema "sys";
// grant select on voyages to phproxy;

define("monetdb_host","localhost");
define("monetdb_port","50001");
define("monetdb_user","phproxy");
define("monetdb_pass","phproxy");
define("monetdb_dbnm","clueweb-segment0");

// inlining php_mapi.inc
/**
   *	Implementation of the MAPI protocol (v9).
   *
   *
   * 	Provides:
   * -  mapi_query($data) {
   * -  mapi_store($data) {
   * -  php_parse_tuples($rows) {
   * -  mapi_connect() {
   * -  mapi_authenticate($user, $passwd, $hash, $salt, $dbname) {
   * -  mapi_read() {
   * -  mapi_write($msg) {
   * -  set_timezone() {
   * -  format_command($cmd) {
   * -  mapi_connect_proxy() {
   * -  mapi_open() {
   * - mapi_close() {
   *
**/

	define("MAX_PACKET_SIZE", 8190); // Maximum packet size

	define("REPLY_SIZE", "250"); // Set the initial reply size

	define("Q_TABLE", "1"); // SELECT operation
	define("Q_UPDATE", "2"); // INSERT/UPDATE operations
	define("Q_CREATE", "3"); // CREATE/DROP TABLE operations
	define("Q_TRANSACTION", "4"); // TRANSACTION
	define("Q_PREPARE", "5");
	define("Q_BLOCK", "6"); // QBLOCK message

	define("MSG_REDIRECT", "^"); // auth redirection through merovingian
	define("MSG_QUERY", "&");
	define("MSG_SCHEMA_HEADER", "%");
	define("MSG_INFO", "!"); // info response from mserver
	define("MSG_TUPLE", "[");
	define("MSG_PROMPT", "");

	define("PROTOCOL_v9", 9); // supported protocol(s)

	define("LANG_SQL", "sql");

	define("MEROVINGIAN", "merovingian");
	define("MSERVER", "mserver");
	define("MONETDB", "monetdb");

	define('MAX_MEROVINGIAN_ITER', 10); // Maximum number of iterations during proxied auth through merovingian

	define('PLACEHOLDER', '?'); // Used for string escaping.

	/**
	* $connection_pool contains a list of the active database connections
	*/
	$connection_pool = array();

	/**
	* Stores the last error reported by the server
	*/
	$last_error = "";


	/**
	* Execute an SQL query and return the resulting handle by reference.
	*/
	function mapi_execute($conn=NULL, $query) {
		global $connection_pool;
		global $rows;
		/**
		* Query handle
		*
		* "conn" - id of the connection that fired the query
		* "header" - table header
		* "record_set" - retrieved record set (if present)
		* "operation" - query type
		*
		*/

		// if multiple connections are present, require the user to specify which one to use.
		if ($conn == NULL) {
			return FALSE;
		} else if (($socket = $conn["socket"]) == NULL) {
			return FALSE;
		}

		/* Fire the query and read back the response */
		$buf = mapi_write($socket, format_query($query, $conn["lang"]));
		$data = mapi_read($socket);

		if ($conn['lang'] == LANG_SQL) {
			$handle = array("conn" => "", "header" => array(), "query" => "", "record_set" => array(), "operation" => -1, "last_row" => 0);
			$handle["conn"] = $conn["id"];

			if ( ($operation = mapi_store($data, $handle)) == FALSE) {
				return FALSE;
			}

			/* The query produced a record set */
			if ($operation == Q_TABLE || $operation == Q_BLOCK) {
				// fetch the whole result set
				if ($handle["query"]["index"] < $handle["query"]["rows"]) {
					mapi_fetch_next($conn, $handle);
				}
			}

			if ($handle == NULL) {
				return FALSE;
			}

			return $handle;
		}
	}

	function mapi_fetch_next($conn, &$handle){
		if ($conn['socket'] == NULL){
			return FALSE;
		}
		$socket = $conn['socket'];

		$offset = 10;
		while ($handle["query"]["index"] < $handle["query"]["rows"]) {
			// export a new window
			$left_rows = $handle["query"]["rows"] - $handle["query"]["index"];

			$exp_size = min($offset, $left_rows);
			$offset += 100;
			mapi_write($socket, format_command("export " . $handle["query"]["id"] . " " . $handle["query"]["index"] . " " . $exp_size));
			$data = mapi_read($socket);

			if ( ($operation = mapi_store($data, $handle)) == FALSE ) {
				return FALSE;
			}

			$handle["query"]["index"] += $exp_size;
			$handle["operation"] = $operation;
		}

		return $handle;
	}

	function mapi_store($data, &$handle) {
		global $last_error;
		// global $rows;

		$data = explode("\n", $data);

		$operation = "";
		$header = ""; // store temporary header informations;

		$rows = ""; // stores (partially) retrieved rows
		foreach ($data as $row) {
			/*
			 	PHP5.2 complains when $row[0] is accessed with:

				Notice: Uninitialized string offset: 0

				In order to avoid the E_NOTICE error substr($row, 0, 1) is used
				to access the first character of a string

			*/
			if (substr($row, 0, 1) == MSG_QUERY) {
				if ($row[1] == Q_TABLE) {
					$operation = Q_TABLE;
					// save info about the query
					$fields = explode(" ", $row);
					$handle["query"] = array("id" => $fields[1], "rows" => $fields[2], "cols" => $fields[3], "index" => $fields[4]);

				} else if ($row[1] == Q_UPDATE) {
					$operation = Q_UPDATE;
					$fields = explode(" ", $row);
					$handle["query"] = array("affected" => $fields[1]);
				} else if ($row[1] == Q_CREATE) {
					$operation = Q_CREATE;
				} else if ($row[1] == Q_TRANSACTION) {
					$operation = Q_TRANSACTION;
				} else if ($row[1] == Q_BLOCK) {
					// add Q_BLOCK to the record set
					$operation = Q_BLOCK;
				}
			} else if (substr($row, 0, 1) == MSG_SCHEMA_HEADER){
				// process the table header
				$header = $header . $row . "\n";
			} else if (substr($row, 0, 1) == MSG_TUPLE) {
				// process tuples
				$rows .= $row;
			} else if (substr($row, 0, 1) == MSG_PROMPT) {
				fast_array_merge($handle["record_set"], php_parse_tuples($rows));
			} else if (substr($row, 0, 1) == MSG_INFO) {
				$last_error = $row;
				return FALSE;
			}
		}
		/*
		if ($record_set != "") {
			//$handle["record_set"] = array_merge($handle["record_set"], $record_set);
			$handle["record_set"] = $handle["record_set"] . $record_set;
		}*/

		if ($header != "") {

			$handle["header"] = php_parse_header($header);
			/* Store the number of fields returned by the dataset */
		   	if ($operation == Q_TABLE || $operation == Q_BLOCK ) {
				$handle["query"]["fields"] = count($handle["header"]["fields"]);
			}
		}

		$handle["operation"] = $operation;

		return $operation;
	}

	/* Concatenates two arrays in place */
	function fast_array_merge(&$a1, $a2) {
		foreach ($a2 as $row) {
			$a1[] = $row;
		}
	}

	function php_parse_tuples($rows) {
		//$parsed_rows = "";

		$rows = rtrim($rows, "\t]");
		return explode("\t]", $rows);
//		print_r($parsed_rows);
		/*
		foreach ($rows as &$row) {

			$row = ltrim($row, "[ ");
			$row = explode(",\t",  $row);

			foreach ($row as &$field) {
				$field = stripslashes($field);
				// strip left/right \" chars and right ','
				$field = rtrim($field, '"');
				$field = ltrim($field, '"');
			}
			$parsed_rows[] = $row;
		}
		*/
	//	return $parsed_rows;
	}

	function php_parse_row($row) {
		$row = ltrim($row, "[ ");
		$row = explode(",\t",  $row);
		foreach ($row as &$field) {
			if ($field == "NULL") {
				$field = NULL;
			}
			else {

				$field = stripslashes($field);
				// strip left/right \" chars and right ','
				$field = rtrim($field, '"');
				$field = ltrim($field, '"');
			}
		}
		return $row;
	}


	function php_parse_header($header) {
		$header = explode("\n", $header);
		$name = $header[0];

		$header_array = array();

		/* Field names */
		$header[1] = ltrim($header[1], "% ");
		$header[1] = explode("#", $header[1]);
		$header_array["fields"] = explode(",\t", $header[1][0]);

		/* Field types */
		$header[2] = ltrim($header[2], "% ");
		$header[2] = explode("#", $header[2]);
		$header_array["types"] = explode(",\t", $header[2][0]);

		return $header_array;
	}

	/*
		$conn_opts is an array containing:
		username =>
		password =>
		host 	 =>
		port 	 =>
		database =>
		hash	 =>

	*/
	function mapi_connect(&$socket, &$options, $merovingian_iter=NULL) {
		global $last_error;
		global $connection_pool;

		$host 	= $options["host"];
		$port 	= $options["port"];

		$user 	= $options["username"];
		$passwd = $options["password"];
		$hash 	= $options["hashfunc"];
		$dbname = $options["database"];

		$lang 	= $options["lang"];


		/* No merovingian redirect. Perform an actual connection. */
		if ($merovingian_iter == NULL) {
			if (socket_connect($socket, $host, $port) == FALSE) {
            	$last_error = socket_strerror(socket_last_error());
              	exit();
            }
		}


		// get server challenge
		$challenge = mapi_read($socket);

		/*
			Array
			(
			    [0] => void
			    [1] => merovingian
			    [2] => 8
			    [3] => plain
			    [4] => LIT
			)

		*/
		$credentials = explode(":", $challenge);
		$algos = explode(',', $credentials[3]);


		// $challenge[0] contains the salt
		if ($credentials[2] == PROTOCOL_v9) {
			// $credentials[5] contains pwhash
			// $credentials[0] contains the hash
			mapi_authenticate_v9($socket, $user, $passwd, $hash, $algos, $credentials[0], $dbname, $credentials[5]);
		} else {
			$last_error = "Protocol " . $credentials[2] . " not supported. Aborting.";
			return FALSE;
		}

		$response = mapi_read($socket);

		/* Follow the first redirect */

		if ($response != MSG_PROMPT) {
		// not ready to authenticate yet
			if ($response[0] == MSG_REDIRECT) {
				$redirects = explode("\n", $response);
	                 	/* Follow the first redirect */

				if ( ($redirects[0] == "") || (substr($redirects[0], 0, 6) != "^mapi:") ) {
					print "Invalid redirect " . $redirects[0] . "\n";
					return FALSE;
				}

				$link = substr($redirects[0], 6, strlen($redirects[0]));
				$redirect_to = parse_url($link);
				//print_r($redirect_to);
				if ($redirect_to['scheme'] == MEROVINGIAN) {
					if ($merovingian_iter < MAX_MEROVINGIAN_ITER) {
						$merovingian_iter++;
						if (mapi_connect($socket, $options, $merovingian_iter) == FALSE) {
							return FALSE;
						}
					} else {
						$last_error = "Maximum number of merovingian iterations exceeded during authentication\n";
						return FALSE;
					}
				}
			 	else if($redirect_to['scheme'] == MONETDB ) {

					$options['host'] = $redirect_to['host'];
					$options['port']     = $redirect_to['port'];
					$options['database'] = ltrim($redirect_to['path'], '/');

					socket_close($socket);
					$socket = mapi_open();
					if (mapi_connect($socket, $options) == FALSE) {
						return FALSE;
					}

			    } else {
					print $response;
					return FALSE;
				}

			} else if ($response[0] == MSG_INFO) {
				$last_error = $response;
				return FALSE;
			}
		}

		return TRUE;
	}

	/* Hash function names have to be uppercase */
	function mapi_authenticate_v9($socket, $user, $passwd, $hash, $algos, $salt, $dbname, $pwhash) {
		$auth_string = "";
		if ( (is_array($algos) && (! in_array(strtoupper($hash), $algos)) )  ) 			 {
			$last_error = "Hash function " . $hash . " not supported";
			return FALSE;
		}

		$pwhashsum = hash(strtolower($pwhash), $passwd);
		$hashsum   = hash(strtolower($hash), $pwhashsum . $salt);

		$auth_string = "BIG:" . $user . ":{" . strtoupper($hash) . "}" . $hashsum . ":sql:" . $dbname . ":";

		mapi_write($socket, $auth_string);
	}

	// decode the header and get the requested amount of data
	function mapi_read($socket=NULL) {
		# get the first 2 bytes
		if ( ($header = socket_read($socket, 2)) == FALSE) {
			$last_error = socket_strerror(socket_last_error());
			exit();
		}
		$data = "";

		$chunk_size = ((ord($header[1]) << 7) | (ord($header[0]) >> 1));
		// keep reading until we have everything
		while (strlen($data) < $chunk_size) {
			$data .= socket_read($socket, $chunk_size - strlen($data));
		}


		while ((ord($header[0]) & 1) == 0 ) {
			if ( ($header = socket_read($socket, 2)) == FALSE) {
				$last_error = socket_strerror(socket_last_error());
				exit();
			}

			$chunk_size = ( ((ord($header[1]))  << 7) | (ord($header[0]) >> 1) );

			$block = "";
			while (strlen($block) < $chunk_size) {
				if ( ($block .= socket_read($socket, $chunk_size - strlen($block))) == FALSE) {
					$last_error = socket_strerror(socket_last_error());
					exit();
				}
			}


			$data = $data . $block;
		}
		if (strlen($data) == 0) {
			return "";
		}

		return $data;
	}


	// encode data and send it to the server. Returns the number of bytes sent.
	function mapi_write($socket=NULL, $msg) {
		// print "Msg_len: " . strlen($msg) . "\n";
		global $last_error;

		$fb = 0;
		$sb = 0;

		$pos = 0;
		$data = "";

		$buf = 0;

		$is_final = FALSE;
		while (! $is_final) {
			$data  = substr($msg, $pos, min(MAX_PACKET_SIZE, (strlen($msg) - $pos))  );
			$pos += strlen($data);

			$end = 0; // more packets will follow
			if ( (strlen($msg) - $pos) == 0) {
				$is_final = TRUE;
				$end = 1;
			}

			$fb = (strlen($data) << 1) | $end;

			/**
			  * socket_write() does not guarantee all data to be transmitted.
			  * Make sure that the buffer is flushed.
			*/

			if ( ($buf = socket_flush($socket, pack("v", $fb) . $data)) == FALSE) {
				$last_error = socket_strerror(socket_last_error());
				return -1;
			}

		}
		return $buf;
	}

	function set_timezone($socket=NULL) {
		global $last_error;

		$tz_offset = "'" . date('P') . "'"; /* Difference to Greenwich time (GMT) with colon between hours and minutes */

		$query = "SET TIME ZONE INTERVAL " . $tz_offset . " HOUR TO MINUTE";

		$buf = mapi_write($socket, format_query($query, LANG_SQL)); // set_timezone is called only when connecting to an sql db

		$response = mapi_read($socket);

		if ($response == "") {
			return TRUE;
		} else {

			$last_error = $response;
			return $response;
		}
	}

	function format_command($cmd) {
		return "X" . $cmd;
	}


	function mapi_connect_proxy(&$options) {
		global $last_error;
		global $connection_pool;
		global $pconnect_count;

		$merovingian_iter = 0;

		/**
		  * When connecting, the function would first try to find a (persistent) link that's already
		  * open with the same host, username and password. If one is found, an identifier for it will be returned
		  * instead of opening a new connection.
		  * TODO: move this check to mapi_connect() to deal with options arrays rewritten by redirects.
		*/
		if ($options['persistent'] == TRUE) {
			if (count($connection_pool) > 0) {
				foreach ($connection_pool as $conn) {
					if ( ($conn["persistent"] == TRUE) && ($conn["dbname"] == $options['database']) && ($conn['username'] == $options['username']) && ($conn["password"] == hash($options['hashfunc'], $options['password']) && ($conn['host']) == $options['host']) && ( $conn['port'] == $options['port'] ) ) {
						return $conn;
					}
				}
			}
		}

		$socket	= mapi_open();

		if ( mapi_connect($socket, $options, $merovingian_iter) == TRUE ) {
			/* Connected */

			// Create a new connection instance and insert an entry in the connections table
			$id = mapi_generate_id();
			if ($options['lang'] == LANG_SQL) {


				/*
					PHP requires a timezone to be specified in the configuration environment; if not specified
					either in php.ini or via date_default_timezone_set(), a default is set 'Europe/Berlin'.

					PHP complains in case we query the OS to get system's timezone (E_STRICT error).

					To avoid unexpected behaviours and warnings at execution time we set a timezone on mserver
					only if PHP interpreter is aware of it (php.ini contains a date.timezone entry).
				*/
				if (ini_get("date.timezone")) {
					set_timezone($socket); // set the timezone according to the system's configuration
				}

				// export the reply size (max number of tuples returned at query executions)
				mapi_write($socket, format_command("reply_size " . REPLY_SIZE));
				if (strlen($response = mapi_read($socket)) > 0 ) {
					// something went wrong
					$last_error = $response;
					return FALSE;
				}
			} else {
				return FALSE;
			}
		} else {
			socket_close($socket);
			return FALSE;
		}

		$connection = array("id" => $id, "socket" => $socket, "host" => $options["host"], "port" => $options["port"], "dbname" => $options['database'], "username" => $options["username"], "password" => hash($options['hashfunc'], $options["password"]), "transactions" => array(), "persistent" => $options["persistent"], "lang" => $options['lang']);

		$connection_pool[] = $connection;

		return $connection;
	}

	function mapi_connected($conn=NULL) {
		global $last_error;
		global $connection_pool;

		if ($conn == NULL) {
			return FALSE;
		} else {
			return mapi_ping($conn);
		}

		return FALSE;
	}

	function mapi_ping($conn=NULL) {
		if ($conn != NULL) {
			switch($conn['lang']) {
				case LANG_SQL:
					$res = mapi_execute($conn, "select true;");
					break;
			}
			if ($res != NULL) {
				if (!is_array($res['query']) ||
					!array_key_exists('id', $res['query']) ||
					mapi_free_result($conn['id'], $res['query']['id']))
			   	{
					return TRUE;
				}
			}
		}
		return FALSE;
	}

	/* Returns a pointer to the current (last initialized) connection */
	function mapi_get_current_conn() {
		global $connection_pool;

		if (count($connection_pool) > 0) {
			return end($connection_pool);
		} else {
			return FALSE;
		}
	}

	/* Returns a socket */
	function mapi_open() {
		$socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
		if ($socket == FALSE) {
			$last_error = socket_strerror(socket_last_error());
			return FALSE;
		}

		return $socket;
	}

	function mapi_close($conn=NULL) {
		global $connection_pool;

		/* TODO: start closing connection at the end of the array! */
		if ($conn == NULL) {
			if ((count($connection_pool) == 1) && ($connection_pool[0]["persistent"]) == FALSE) {
				$socket = $connection_pool[0]["socket"];
				socket_close($socket);

				foreach ($connection_pool as $field) {
					if (isset($field)) {
						unset($field);
					}
				}
				unset($connection_pool);

				return TRUE;
			}

		} else {
			$socket = $conn["socket"];
			socket_close($socket);

			/* remove the $conn from the pool */
			// Create anonymous callback function to filter results for connection.
			$function_body = 'return ( $input[\'id\'] != "'.$conn["id"].'" );';
			$function_name = create_function('$input', $function_body);
			// Filter the results array using the anonymous callback function.
			$connection_pool = array_filter($connection_pool, $function_name);

			if (isset($conn)) {
				foreach ($conn as $field) {
					if (isset($field)) {
						unset($field);
					}
				}
				unset($conn);
			}

			return TRUE;
		}

		return FALSE;
	}

	function mapi_free_result($conn_id, $res_id) {
		global $connection_pool;
		global $last_error;
		/* Fetch the connection from the pool */

		$conn = NULL;
		foreach ($connection_pool as $connection) {
			if ($connection["id"] == $conn_id) {
				$conn = $connection;
				break;
			}
		}

		if ($conn == NULL) {
			return FALSE;
		}

		$socket = $conn["socket"];

		/* Send a close command */
		$cmd = "close " . $res_id;

		mapi_write($socket, format_command($cmd));
		$last_error = mapi_read($socket);

		if ($last_error != "") {
			return FALSE;
		}

		return TRUE;
	}


	function mapi_generate_id(){
		global $connection_pool;

		$connections = array();
		if ($connection_pool !== null) {
		    foreach($connection_pool as $conn) {
			$connections[] = $conn["id"];
		    }
		}

		$id = hash("sha1", time());
		if (count($connections) > 0) {
			while (in_array($id, $connections) ) {
				$id = hash("sha1", time());
			}
		}

		return $id;
	}


	function format_query($query, $lang) {
		if ($lang == LANG_SQL) {

			return "s" . $query . ";";
		}
		return FALSE;
	}

	/* Write data through a socket; make sure that the buffer is actually flushed */
	function socket_flush($socket, $data) {
		$buf = 0;
		$bytes = strlen($data);

		if ($socket == NULL) {
			return FALSE;
		}

		while ( ($bytes - $buf) > 0 ) {
			$buf += socket_write($socket, substr($data, $buf, $bytes), $bytes - $buf);
			//print "Buf: " . $buf . "\n";

			if ($buf == FALSE) {
				return FALSE;
			}

		}
		return $buf;
	}

	function mapi_quote($string, $size) {
		$quoted_string = ""; # upper bound: $size * 2 + 1

		$index = 0; // current position in the original string
		$t = 0; // current position in the quoted string

		/* Parse the original string character by character and copy it in a new buffer escaping characters */
        while ($size < 0 ? $quoted_string[$t] : $size > 0) {
                if ($size > 0)
                        $size--;
                switch ($string[$index]) {
                	case '\n':
                        $quoted_string[$t++] = '\\';
                        $quoted_string[$t++] = 'n';
                        break;
                	case '\t':
                        $quoted_string[$t++] = '\\';
                        $quoted_string[$t++] = 't';
                        break;
                	case PLACEHOLDER:
                        $quoted_string[$t++] = '\\';
                        $quoted_string[$t++] = PLACEHOLDER;
                        break;
                	case '\\':
                        $quoted_string[$t++] = '\\';
                        $quoted_string[$t++] = '\\';
                        break;
                	case '\'':
                        $quoted_string[$t++] = '\'';
                       break;
                	case '\"':
                        $quoted_string[$t++] = '\\';
                        $quoted_string[$t++] = '"';
                        break;
                	case '\0':
                        $quoted_string[$t++] = '\\';
                        $quoted_string[$t++] = '0';
                        break;
                default:
                        $quoted_string[$t++] = $string[$index];
                        break;
                }
                $index++;
                /* also deal with binaries */
        }

		if (is_array($quoted_string)) {
			$quoted_string = implode($quoted_string);
		}
        return $quoted_string;
	}


// inlining php_monetdb.php
	/**
	* php_monetdb.php
	* Implementation of the driver API.
	*
	* This library relies on the native PHP mapi implementation.
	* 
	* This file should be included by all pages that want to make use of a
 	* database connection.
	*
	* Synopsis of the provided functions:
	*
	* function monetdb_connect() *
	* function monetdb_disconnect()  *
    * function monetdb_connected() *
	* function monetdb_query($query)  *
	* function monetdb_fetch_assoc($hdl) *
	* function monetdb_fetch_object($hdl) 
	* function monetdb_num_rows($hdl)  *
	* function monetdb_affected_rows($hdl) * 
	* function monetdb_last_error()  *
	* function monetdb_insert_id($seq)  *
	* function monetdb_quote_ident($str) *
	* function monetdb_escape_string($str) * 
	**/

	/**
	* php_mapi.inc is a native (socket based) php implementation of the MAPI communication protocol.
	*/
	//require 'php_mapi.inc';
	
	/**
	* register a 'monetdb' extension to retain compatibility with wht Cimpl based scripts.
	*/
	
	/**
	 * Opens a connection to a MonetDB server.  
	 * 
	 * @param string language to be used (sql)
	 * @param string hostname to connect to (default is localhost)
	 * @param int    port to use (default is 50000)
	 * @param string username (default is monetdb)
	 * @param string password (default is monetdb)
	 * @param string database to use (default is demo)
	 * @param string hash function to use during authentication (defaults to SHA1) 
	 * @return bool TRUE on success or FALSE on failure 
	 */
	
	function monetdb_connect($lang = "sql", $host = "127.0.0.1", $port = 50000, $username = "monetdb", $password = "monetdb", $database = "demo", $hashfunc = "") {
	 	$options["host"] = $host;
		$options["port"] = $port;

		$options["username"] = $username;
		$options["password"] = $password;
		
		$options["database"] = $database; 
		$options["persistent"] = FALSE;
	
	
	    if ($hashfunc == "") {
		    $hashfunc = "sha1";
	    }
	    
	    if ($lang == "") {
	        $lang = "sql";
	    }
	    
	    $options["hashfunc"] = $hashfunc;
        $options["lang"]     = $lang;
				
		return mapi_connect_proxy($options);
	}

	/**
	 * Opens a persistent connection to a MonetDB server.  
	 * First, when connecting, the function would first try to find a (persistent) link that's already open with the same host, 
	 * username and password. If one is found, an identifier for it will be returned instead of opening a new connection.
	 *
	 * Second, the connection to the SQL server will not be closed when the execution of the script ends. 
	 * Instead, the link will remain open for future use (monetdb_close() will not close links established by monetdb_pconnect()).
	 *
	 * This type of link is therefore called 'persistent'. 
	 *
	 * @param string language to be used (sql)
	 * @param string hostname to connect to (default is localhost)
	 * @param int    port to use (default is 50000)
	 * @param string username (default is monetdb)
	 * @param string password (default is monetdb)
	 * @param string database to use (default is demo)
	 * @param string hash function to use during authentication (defaults to SHA1)
	 * @return bool TRUE on success or FALSE on failure 
	 */
	
  function monetdb_pconnect($lang = "sql", $host = "127.0.0.1", $port = 500000, $username = "monetdb", $password = "monetdb", $database = "demo", $hashfunc = "") {

	 	$options["host"] = $host;
		$options["port"] = $port;

		$options["username"] = $username;
		$options["password"] = $password;
		$options["database"] = $database; 
		$options["persistent"] = TRUE;
		
		if ($hashfunc == "") {
		    $hashfunc = "sha1";
	    }
	    
	    if ($lang == "") {
	        $lang = "sql";
		} else if (strstr($lang, "sql") == $lang) {
			$lang = "sql";
		}
	    
	    $options["hashfunc"] = $hashfunc;
        $options["lang"]     = $lang;
		
		return mapi_connect_proxy($options);
	}

	/**
	 * Disconnects the connection to the database.
	 *
	 * @param resource connection instance
	 */
	function monetdb_disconnect($conn=NULL) {
		$num_args = func_num_args();
		
		if ($num_args == 0) {
			$conn = mapi_get_current_conn();
			mapi_close(NULL);
		} else {
			mapi_close($conn);
		}
	}
	
	/**
	 * Returns whether a connection to the database has been made, and has
	 * not been closed yet.  Note that this function doesn't guarantee that
	 * the connection is alive or usable.
	 *
	 * @param resource connection instance
	 * @return bool TRUE if there is a connection, FALSE otherwise
	 *
	 */
	function monetdb_connected($connection=NULL) {
		$num_args = func_num_args();
		
		if ($num_args == 0){
			return mapi_connected(mapi_get_current_conn());
		}
		
		return mapi_connected($connection);
	}
	
	/**
	 * Executes the given query on the database.
	 *
	 * @param resource connection instance
	 * @param string the SQL query to execute
	 * @return resource a query handle or FALSE on failure
	 */
	function monetdb_query($connection=NULL, $query="") {
		$num_args = func_num_args();
		
		if ($num_args == 1){
			$arg = func_get_arg(0);
			if (is_string($arg)) {
				$conn = mapi_get_current_conn();
				if ($conn != NULL) {
					return mapi_execute($conn, $arg);
				} 
			}
			
		} else {
			return mapi_execute($connection, $query);
		}
		
		return FALSE;
	}
	
	/**
	 * Returns the number of rows in the query result.
	 *
	 * @param resouce the query resource
	 * @return int the number of rows in the result; FALSE if the query did not return any result set
	 */
	function monetdb_num_rows($hdl) {
		if ($hdl["operation"] == Q_TABLE || $hdl["operation"] == Q_BLOCK ) {
			return $hdl["query"]["rows"];
		} else {
			return FALSE;
		}
	}

	/**
	 * Returns the number of fields in the query result.
	 *
	 * @param resouce the query resource
	 * @return int the number of fields in the result; FALSE if the query did not return any result set
	 */	
	function monetdb_num_fields($hdl) {
    	if ($hdl["operation"] == Q_TABLE || $hdl["operation"] == Q_BLOCK ) {
  			return $hdl["query"]["fields"];
  		} else {
  			return FALSE;
  		}
	}
	
	/**
	 * Returns an array containing column values as value. 
	 * For efficiency reasons the array pointer is not reset when calling monetdb_fetch_row
	 * specifying a row value. 
	 *
	 * @param resource the query handle
	 * @param int the position of the row to retrieve
	 * @return array the next row in the query result as associative array or
	 *         FALSE if no more rows exist
	 */
	function monetdb_fetch_row(&$hdl, $row=-1) {	
		global $last_error;
		
		if ($hdl["operation"] != Q_TABLE && $hdl["operation"] != Q_BLOCK ) {
			return FALSE;
		}
	
		if ($row == -1){
		  // Parse the tuple and present it to the user
			$entry = current($hdl["record_set"]);
			
			//advance the array of one position
			next($hdl["record_set"]);

		} else {
			if ($row < $hdl["query"]["rows"]) {
			  /* Parse the tuple and present it to the user*/
				$entry = $hdl["record_set"][$row-1];
			}
			else {
				$last_error = "Index out of bound\n";
				return FALSE;
			}
		}	
		
    if ($entry) {
		  return php_parse_row($entry);
	  }
	  
	  return $entry;
	}

	
	/**
	 * Returns an associative array containing the column names as keys, and
	 * column values as value.
	 *
	 * @param resource the query handle
	 * @param int the position of the row to retrieve
	 * @return array the next row in the query result as associative array or
	 *         FALSE if no more rows exist
	 */	
	function monetdb_fetch_assoc(&$hdl, $row=-1) {
		if ($hdl["operation"] != Q_TABLE && $hdl["operation"] != Q_BLOCK ) {
			return FALSE;
		}
		
		// first retrieve the row as an array
		$fetched_row =  monetdb_fetch_row($hdl, $row);
		
		if ($fetched_row == FALSE) {
			return FALSE;
		}
		
		// now hash the array by field name		
		$hashed = array();

		$i = 0;
		foreach ($hdl["header"]["fields"] as $field) {
			$field = str_replace(" ", "", $field);
			$hashed[$field] = $fetched_row[$i];
			$i++;
		}
		
		return $hashed;
	}


	/**
	 * Returns the result in the given query resource as object one row at a time.  Column
	 * names become members of the object through which the column values
	 * can be retrieved.
	 *
	 * @param resource the query handle
	 * @param int the position of the row to retrieve
	 * @return the query result as object or FALSE if there are no more rows
	 */
	function monetdb_fetch_object(&$hdl, $row=-1)  {
		
		if ($hdl["operation"] != Q_TABLE && $hdl["operation"] != Q_BLOCK ) {
			return FALSE;
		}
		
		if (($row_array =  monetdb_fetch_assoc($hdl, $row)) == FALSE) {
			return FALSE;
		}
		
		$row_object = new stdClass();
		if (is_array($row_array) && count($row_array) > 0) {
			foreach ($row_array as $name=>$value) {
		   		$name = strtolower(trim($name));
		        if (!empty($name)) {
		        	$row_object->$name = $value;
		        }
		     }
		 }
		
		return $row_object;
	}
	
	/**
	* Returns the name of the field at position $field
	*
	* @param resource the query handle
	* @param int field number
	* @return string the field name, FALSE is an error occured.
	*/
	function monetdb_field_name(&$hdl, $field) {
	    if (is_array($hdl) && $field >= 0) {
            if ($hdl["operation"] == Q_TABLE || $hdl["operation"] == Q_BLOCK ) {
                if ($hdl["header"]["fields"] != "" ) {
                    return $hdl["header"]["fields"][$field];
                }
		    }
    	}	
    	return FALSE;
	}
	
	/**
	 * Returns the number of affected rows for an UPDATE, INSERT or DELETE
	 * query.  The number of affected rows typically is 1 for INSERT
	 * queries.
	 *
	 * @param resource the query handle
	 * @return int the number of affected rows, FALSE if the last executed query did not affect any row.
	 */	
	function monetdb_affected_rows($hdl) {
		if ($hdl["operation"] != Q_UPDATE) {
			return FALSE;
		} 
		
		return $hdl["query"]["affected"];
	}
	
	
	/**
	* Check if the query handle contains a result set. Note: the result set may be empty.
	*
	* @param resource the query handle
	* @return bool TRUE if the query contains a result set, FALSE otherwise.
	*/
	function monetdb_has_result($hdl) {
	    if (($hdl["operation"] == Q_TABLE) || ($hdl["operation"] == Q_BLOCK)) {
	        return TRUE;
	    }
	    
	    return FALSE;
	}
	
	/**
	 * Returns the last error reported by the database.
	 *
	 * @return string the last error reported
	 */
	function monetdb_last_error() {
		global $last_error;
		return $last_error;
	}
	
	/**
	 * Generates the next id in the sequence $seq
	 *
	 * @param resource connection instance
	 * @param seq sequence whose next
	 * value we want to retrieve
	 * @return string the ID of the last tuple inserted. FALSE if an error occurs
	 */
	function monetdb_insert_id($connection = NULL, $seq)  {
		$num_args = func_num_args();
		
		if ($num_args == 1) {
			$connection = mapi_get_current_conn();
			$seq = func_get_arg(0);
		}
		
		if (is_string($seq)) {
			$query = "SELECT NEXT VALUE FOR ".monetdb_quote_ident($seq)."";
			$res = monetdb_query($connection, $query);
			$row = monetdb_fetch_assoc($result);
            return($row[$seq]);
		}
		
		return FALSE;
	}
	
	/**
	 * Returns a 'quoted identifier' suitable for MonetDB.
	 * This utility function can be used in queries to for instance quote
	 * names of tables of columns that otherwise would be a mistaken for a
	 * keyword.
	 * NOTE: the given string is currently not checked for validity, hence
	 *       the output of this function may be an invalid identifier.
	 *
	 * @param string the identifier to quote
	 * @return string the quoted identifier
	 *
	 */
	function monetdb_quote_ident($str) {
		return('"'. $str .'"');
	}

	/**
	 * Returns an 'escaped' string that can be used for instance within
	 * single quotes to represent a CHARACTER VARYING object in SQL.
	 *
	 * @param string the string to escape
	 * @return string the escaped string
	 */
	function monetdb_escape_string($str) {
		return mapi_quote($str, strlen($str));
	}
	
	/**
	* Free the result set memory.
	* 
	* @param resource the query handle.
	* @return bool returns TRUE on success or FALSE on failure.
	*
	*/
	function monetdb_free_result(&$hdl) {
		$conn_id = $hdl["conn"];
		$res_id = $hdl["query"]["id"];

		/* Release the result set on server */
		mapi_free_result($conn_id, $res_id);
		
		
		if (isset($hdl) && is_array($hdl)) {
			foreach($hdl as $field) {
				if (isset($field)) {
					unset($field);
				}
			}
			
			unset($hdl);
			
			return TRUE;
		}
		
		return FALSE;
	}
		
	
	/* 
	 * These functions are not present in the original Cimpl implementation
	 */
	
	/**
	* Create a new savepoint ID
	* @param resource connection instance
	* @return bool TRUE if the ID has been correctly generated, FALSE otherwise.
	*/
	function create_savepoint(&$conn) {
		if ($conn != NULL) {
			$index = count($conn["transactions"]);
		
			$id = "monetdbsp" . $index;
			array_push($conn["transactions"], $id);
			
			return TRUE;
		}
		
		return FALSE;
	}
	
	/**
	* Release a savepoint ID.
	* @param resource connection instance
	* @return bool TRUE if the ID has been correctly released, FALSE otherwise.
	*/
	function release_savepoint(&$conn) {
		if ($conn != NULL) { 
			array_pop($conn["transactions"]);
			return TRUE;
		}
		
		return FALSE;
	}

	/**
	* Return the current (last generated) savepoint ID.
	* @param resource connection instance
	* @return string savepoint ID. I no savepoints are available, FALSE is returned.
	*/
	function get_savepoint(&$conn) {
		if (count($conn["transactions"]) == 0) {
			return FALSE;
		}
		
		// return the last element in the array
		return $conn["transactions"][count($conn["transactions"])-1];
	}

	/**
	* Sets auto commit mode on/off
	* @param resource connection instance
	* @param bool TRUE to turn auto commit on, FALSE to turn it off.
	* @return bool TRUE is auto commit mode was correctly set. FALSE otherwise.
	*/
	function auto_commit($conn, $flag=TRUE) {
		if ($conn["socket"] != NULL) {
			$cmd = "auto_commit " . $flag;
			mapi_write($conn["socket"], format_command($cmd));
			
			return TRUE;
		}
		return FALSE;
	}


function err($code,$str) {
	http_response_code($code);
	header("Content-Type: text/plain");
	die($str."\r\n");
}

// you should not have to change stuff below.
// require 'phplib/php_monetdb.php';
$db = monetdb_connect("sql",monetdb_host,monetdb_port,monetdb_user,monetdb_pass,monetdb_dbnm);
if (!$db) {
	err(400,"Could not connect to database: ".monetdb_last_error());
}

$query = $_REQUEST['q'];
// check if the query is at least there
if (trim($query) == "") {
	err(400,"Missing query GET/POST parameter (?q=SELECT...)");
}

if (isset($_REQUEST["callback"]) && !empty($_REQUEST["callback"])) {
	$hasJsonp = true;
	$jsonp = $_REQUEST["callback"];
	if (!ereg("^[[:alnum:]_]+$",$jsonp)) {
		err(400,"Invalid callback request parameter");
	}
}

// actual querying
$res = monetdb_query($db, monetdb_escape_string($query));

if (!$res) {
	err(400,"Invalid query: ".monetdb_last_error());
}

// construct result emtadata
$json = array();
$json["metadata"] = $res["header"];
$json["metadata"]["counts"] = $res["query"];
$json["metadata"]["query"] = $query;
$json["results"] = array();

// ok, construct result json. Simply fetch each row as an assoc array and append
while ( $row = monetdb_fetch_assoc($res) ){
	$json["results"][] = $row;
}

// encode result correctly, depending on whether we want jsonp or not
if ($hasJsonp) {
	header("Content-Type: application/javascript");
	print $jsonp ."(".json_encode($json).");";
} else {
	header("Content-Type: application/json");
	print json_encode($json);
}
