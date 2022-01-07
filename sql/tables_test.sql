SELECT COUNT(*) FROM customer_courier_chat_messages;

SELECT * FROM customer_courier_chat_messages LIMIT 2;

SELECT EXISTS (
   SELECT FROM information_schema.tables 
   WHERE table_name IN ('customer_courier_conversations', 'customer_courier_conversations', 'customer_courier_chat_messages')
   );

SELECT 
   table_name, 
   column_name, 
   data_type 
FROM 
   information_schema.columns
WHERE 
   table_name = 'customer_courier_conversations';