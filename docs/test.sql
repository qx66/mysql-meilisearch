
create database test default charset utf8mb4;

create table user (`uuid` varchar(50), `name` varchar(20), `age` int);

create table docs (`uuid` varchar(50), `name` varchar(20), content text);

insert into user (uuid, name, age) value  ('5ca17e25-8683-4832-9ee1-974d6f27aa3a', 'qx', 30);


insert into docs (uuid, name, content) value ('5ca17e25-8683-4832-9ee1-974d6f27aa3a', '我是一个人', '你麻痹');