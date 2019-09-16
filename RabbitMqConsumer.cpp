#include "RabbitMQ.h"

int InitConnection(amqp_connection_state_t &conn)
{
	int status;
	amqp_socket_t *socket;
	amqp_rpc_reply_t Reply;

	status = 0;
	socket = NULL;

	conn = amqp_new_connection();
	if (NULL == conn || 0 == conn)
	{
		printf("\n amqp_new_connection failed");
		return 1;
	}

	socket = amqp_tcp_socket_new(conn);
	if (NULL == socket)
	{
		printf("\n amqp_tcp_socket_new failed");
		return 1;
	}


	status = amqp_socket_open(socket, HOSTNAME, PORT);
	if (AMQP_STATUS_OK != status)
	{
		printf("\n amqp_socket_open failed");
		return 1;
	}

	memset(&Reply, 0, sizeof(Reply));
	Reply = amqp_login(conn, "/", 0, 131072, 0, AMQP_SASL_METHOD_PLAIN, USERNAME, PASSWORD);
	if (AMQP_RESPONSE_NORMAL != Reply.reply_type)
	{
		printf("\n amqp_login failed err => (%s)", amqp_error_string2(Reply.library_error));
		return 1;
	}

	amqp_channel_open_ok_t *Channel = NULL;
	Channel = amqp_channel_open(conn, 1);
	if (NULL == Channel)
	{
		printf("\n amqp_channel_open failed");
		return 1;
	}

	Reply = amqp_get_rpc_reply(conn);
	if (AMQP_RESPONSE_NORMAL != Reply.reply_type)
	{
		printf("\n amqp_get_rpc_reply failed");
		return 1;
	}

	std::cout<<"\nEstablished Connection...";

	return 0;
}


int DeInitConnection(amqp_connection_state_t &conn)
{
	int status;
	amqp_rpc_reply_t Reply;

	Reply = amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
	if (AMQP_RESPONSE_NORMAL != Reply.reply_type)
	{
		printf("\n amqp_channel_close failed");
		return 1;
	}

	Reply = amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
	if (AMQP_RESPONSE_NORMAL != Reply.reply_type)
	{
		printf("\n amqp_connection_close failed");
		return 1;
	}

	status = amqp_destroy_connection(conn);
	if (AMQP_STATUS_OK != status)
	{
		printf("\n amqp_basic_publish failed");
		return 1;
	}

	std::cout<<"\nTerminated Connection...";

	return 0;
}

int Consume(amqp_connection_state_t &conn, const char *exchange_name, const char *exchange_type, const char *queuename, const char *binding_key)
{

	int status;
	char response[100];
	amqp_rpc_reply_t reply;
	amqp_message_t message;
	amqp_envelope_t envelope;
	amqp_channel_t channel = 1;
	amqp_boolean_t passive = 1;
	amqp_boolean_t durable = 1;
	amqp_boolean_t internall = 0;
	amqp_boolean_t exclusive = 0;
	amqp_boolean_t auto_delete = 0;

	amqp_queue_declare_ok_t *que;
	amqp_queue_bind_ok_t *quebind;
	amqp_exchange_declare_ok_t *exch;

	//
	// Declaring exchange
	//
	exch = amqp_exchange_declare(
			conn,
			channel,
			amqp_cstring_bytes(exchange_name),
			amqp_cstring_bytes(exchange_type),
			passive,
			durable,
			auto_delete,
			internall,
			amqp_empty_table
			);


	reply = amqp_get_rpc_reply(conn);
	if(reply.reply_type != AMQP_RESPONSE_NORMAL)
	{
		amqp_connection_close_t *m = (amqp_connection_close_t *) reply.reply.decoded;
		printf("%s: server connection error %d, message: %.*s\n", "Error declaring exchange", m->reply_code,(int) m->reply_text.len, (char *) m->reply_text.bytes);
		if (404 == m->reply_code)
		{
			//exchange does not exists
			return 2;
		}
		return 1;
	}

	//
	// Declaring queue
	//

	que = amqp_queue_declare(
			conn,
			channel,
			amqp_cstring_bytes(queuename),
			passive,
			durable,
			exclusive,
			auto_delete,
			amqp_empty_table
			);

	reply = amqp_get_rpc_reply(conn);
	if(reply.reply_type != AMQP_RESPONSE_NORMAL)
	{
		amqp_connection_close_t *m = (amqp_connection_close_t *) reply.reply.decoded;
		printf("%s: server connection error %d, message: %.*s\n", "Error declaring queue", m->reply_code,(int) m->reply_text.len, (char *) m->reply_text.bytes);
		if (404 == m->reply_code)
		{
			//queue does not exists
			return 2;
		}
		return 1;
	}


	//
	// Binding to queue
	//
	quebind = amqp_queue_bind(
		conn,
		channel,
		amqp_cstring_bytes(queuename),
		amqp_cstring_bytes(exchange_name),
		amqp_cstring_bytes(binding_key),
		amqp_empty_table
		);

	reply = amqp_get_rpc_reply(conn);
	if (AMQP_RESPONSE_NORMAL != reply.reply_type)
	{
		printf("\n amqp_get_rpc_reply failed");
		return 1;
	}

	if (que->message_count == 0)
	{
		//queue empty poll after some time
		printf("\nQueue Empty");
		return 3;
	}

	while (que->message_count > 0)
	{
		reply = amqp_basic_get(conn, channel, amqp_cstring_bytes(queuename), 1);
		reply = amqp_get_rpc_reply(conn);
		if (AMQP_RESPONSE_NORMAL != reply.reply_type)
		{
			printf("\n amqp_get_rpc_reply failed");
			return 1;
		}

		reply = amqp_read_message(conn, channel, &message, 0);
		reply = amqp_get_rpc_reply(conn);
		if (AMQP_RESPONSE_NORMAL != reply.reply_type)
		{
			printf("\n amqp_get_rpc_reply failed");
			return 1;
		}

		strncpy(response, (const char *)message.body.bytes, message.body.len);
		response[message.body.len] = '\0';
		printf("\n consumed message  (%s)", response);

		amqp_destroy_message(&message);
		--que->message_count;
	}


	return 0;
}

int main(int argc, char const *const *argv) 
{

	int status;
	char *message;
	const char *exchange;
	const char *queuename;
	const char *bindingkey;
	const char *exchangeType;

	amqp_rpc_reply_t Reply;
	amqp_envelope_t envelope;
	amqp_socket_t *socket = NULL;
	amqp_connection_state_t conn;

	if (5 != argc)
	{
		printf("consumer.exe queuename bindingkey exchange exchangetype");
		return 0;
	}
	//sdkqueue sdkkey sdkexchange direct
	queuename = argv[1];//"sdkqueue";
	bindingkey = argv[2];//"sdkkey";
	exchange = argv[3];//"sdkexchange";
	exchangeType = argv[4];//"direct";
	//message = "Test sdk message";

	//
	//	Init Connection
	//
	status = InitConnection(conn);
	if (0 != status)
	{
		printf("\nInitConnection() Failed");
		return 1;
	}

	//
	//	consume message
	//
	for(;;)
	{

		status = Consume(conn, exchange, exchangeType, queuename, bindingkey);
		if (3 == status)
		{
			Sleep(2000);
			continue;
		}

		if (0 != status)
		{
			printf("\nFailed to Consume message");
			DeInitConnection(conn);
			return 1;
		}

	}

	//
	//	DeInit Connection
	//
	status = DeInitConnection(conn);
	if (0 != status)
	{
		printf("\nDeInitConnection() Failed");
		return 1;
	}

	return 0;
}

