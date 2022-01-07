-- ddl statement to create customer_courier_chat_messages table if it does not exists
CREATE TABLE IF NOT EXISTS customer_courier_chat_messages(
  sender_app_type text,
  customer_id integer,
  from_id integer,
  to_id integer,
  chat_started_by_message bool,
  order_id integer,
  order_stage text,
  courier_id integer,
  message_sent_time timestamp
);

-- ddl statement to create orders table if it does not exists
CREATE TABLE IF NOT EXISTS orders(
  order_id integer,
  city_code text,
  PRIMARY KEY (order_id)
);

-- ddl statement to create customer_courier_conversations table if it does not exists
CREATE TABLE IF NOT EXISTS customer_courier_conversations(
  order_id integer,
  city_code text,
  first_courier_messsage timestamp,
  first_customer_messsage timestamp,
  num_messages_courier integer,
  num_messages_customer integer,
  first_message_by text,
  conversation_started_at timestamp,
  first_responsetime_delay_seconds timestamp,
  last_message_time timestamp,
  last_message_order_stage text,
  CONSTRAINT fk_order
      FOREIGN KEY(order_id) 
	  REFERENCES orders(order_id)
	  ON DELETE SET NULL,
  CONSTRAINT unique_order_conversation UNIQUE(order_id)
);

-- insert ddl statement, to insert records into the customer_courier_chat_messages table
INSERT INTO customer_courier_chat_messages (sender_app_type, customer_id, from_id, to_id, chat_started_by_message, order_id, order_stage, courier_id, message_sent_time)
                                            VALUES
                                           ('Customer iOS', 17071099, 17071099, 16293039, FALSE, 59528555, 'PICKING_UP', 16293039, '2019-08-19 8:01:47'),
                                           ('Customer iOS', 17071099, 16293039, 17071099, FALSE, 59528555, 'ARRIVING', 16293039, '2019-08-19 8:01:04'),
                                           ('Customer iOS', 17071099, 17071099, 16293039, FALSE, 59528555, 'PICKING_UP', 16293039, '2019-08-19 8:00:04'),
                                           ('Courier Android', 12874122, 18325287, 12874122, TRUE, 59528038, 'ADDRESS_DELIVERY', 18325287, '2019-08-19 7:59:33');
                                           
                                           