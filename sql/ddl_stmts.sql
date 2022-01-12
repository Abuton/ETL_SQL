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


-- using ctes to aggregate the customer_courier_chat_messages to get values for each field in the
-- customer_courier_conversations table
WITH orders AS (
  -- get the unique order_id
  select distinct order_id as orders_id from customer_courier_chat_messages
),
number_of_message_courier as (
  -- get count of messages a courier send per order
  select order_id, count(sender_app_type) as number_msg_courier from customer_courier_chat_messages where substring(sender_app_type, 1,7) = 'Courier' group by order_id
),
number_of_message_customer as (
  -- get count of messages a cutomer send per order
  select order_id, count(sender_app_type) as number_msg_customer from customer_courier_chat_messages where substring(sender_app_type, 1,8) = 'Customer' group by order_id
),
first_courier_msg as (
  -- get the time a courier sends the first messages per order
  select order_id, min(message_sent_time) as first_courier_msg from customer_courier_chat_messages where substring(sender_app_type, 1,7) = 'Courier' group by order_id
),
first_customer_msg as (
  -- get the time a customer sends the first messages per order
  select order_id, min(message_sent_time) as first_customer_msg from customer_courier_chat_messages where substring(sender_app_type, 1,8) = 'Customer' group by order_id
),
first_msg_by as (
  -- get who sends the first message per order
  select order_id, substring(sender_app_type, 1,8)  as first_msg_by from customer_courier_chat_messages where message_sent_time = (select min(message_sent_time) from customer_courier_chat_messages) group by order_id,sender_app_type
),
conversation_started_by as (
  -- get the time at  which the first message was sent per order
  select order_id, min(message_sent_time) as conversation_started_at from customer_courier_chat_messages group by order_id
),

last_message_time as (
  -- get get the time at  which the last message was sent per order
  select order_id, max(message_sent_time) as last_message_time from customer_courier_chat_messages group by order_id
),
last_msg_order_stage as (
  -- get the order_stage per order when the last message was sent
  select order_id, order_stage as last_message_order_stage from customer_courier_chat_messages where message_sent_time = (select max(message_sent_time) from customer_courier_chat_messages) group by order_id, order_stage
)

-- populate the customer_courier_conversation table with the result of the cte joins
INSERT INTO customer_courier_conversations
 (order_id, first_courier_messsage, first_customer_messsage, num_messages_courier, num_messages_customer, first_message_by, conversation_started_at, last_message_time, last_message_order_stage)
  
SELECT o.orders_id, first_courier_msg, first_customer_msg, number_msg_courier, number_msg_customer, first_msg_by, conversation_started_at, last_message_time, last_message_order_stage FROM
  orders o join number_of_message_courier on o.orders_id = number_of_message_courier.order_id 
    join number_of_message_customer on number_of_message_customer.order_id = o.orders_id 
    join first_courier_msg on first_courier_msg.order_id = o.orders_id
    join first_customer_msg on first_customer_msg.order_id = o.orders_id
    join first_msg_by on first_msg_by.order_id = o.orders_id
    join conversation_started_by on conversation_started_by.order_id = o.orders_id
    join last_message_time on last_message_time.order_id = o.orders_id
    join last_msg_order_stage on last_msg_order_stage.order_id = o.orders_id;
    
select * from customer_courier_conversations;