# del_gmail_mails

For complete information about the script please go through the following blog link

# Prerequisites

Create the following directory on the same path where script resides:
`mkdir mails_info`

Before running the script please create the following database and tables.
Below are the mysql statements 

DB Statements
========
create database `mysql_mails`;

CREATE TABLE `message_ids` (
  `id` int NOT NULL AUTO_INCREMENT,
  `message_id` varchar(256) DEFAULT NULL,
  `label` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
);
