 CREATE TABLE `moving_average` (
  `examination_id` int(11) NOT NULL,
  `date_time` datetime NOT NULL,
  `avg_value` decimal(10,2) NOT NULL,
  PRIMARY KEY (`examination_id`,`date_time`)
