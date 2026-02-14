/* Create table r_pv_train in transport schema*/
CREATE TABLE `transport`.`r_pv_bus` (
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT,
	`year_month` DATE NULL DEFAULT NULL,
	`day_type` VARCHAR(50) NULL DEFAULT NULL,
	`time_per_hour` INT(11) NULL DEFAULT NULL,
	`pt_type` VARCHAR(50) NULL DEFAULT NULL,
	`pt_code` VARCHAR(50) NULL DEFAULT NULL,
	`total_tap_in_volume` INT(11) NULL DEFAULT NULL,
	`total_tap_out_volume` INT(11) NULL DEFAULT NULL,
	PRIMARY KEY (`id`) USING BTREE
)
ENGINE=InnoDB
;
