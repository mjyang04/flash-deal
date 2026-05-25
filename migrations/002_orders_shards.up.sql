-- M3: 4 logical DBs as schemas in the single dev MySQL instance.
CREATE DATABASE IF NOT EXISTS flashdeal_0;
CREATE DATABASE IF NOT EXISTS flashdeal_1;
CREATE DATABASE IF NOT EXISTS flashdeal_2;
CREATE DATABASE IF NOT EXISTS flashdeal_3;

GRANT ALL ON flashdeal_0.* TO 'flashdeal'@'%';
GRANT ALL ON flashdeal_1.* TO 'flashdeal'@'%';
GRANT ALL ON flashdeal_2.* TO 'flashdeal'@'%';
GRANT ALL ON flashdeal_3.* TO 'flashdeal'@'%';
FLUSH PRIVILEGES;
