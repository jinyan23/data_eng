/* Create table r_pv_train in transport */
CREATE TABLE `r_pv_train` (
	`id` BIGINT(20) NOT NULL AUTO_INCREMENT,
	`year_month` DATE NULL DEFAULT NULL,
	`day_type` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
	`time_per_hour` INT(11) NULL DEFAULT NULL,
	`pt_type` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
	`pt_code` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
	`total_tap_in_volume` INT(11) NULL DEFAULT NULL,
	`total_tap_out_volume` INT(11) NULL DEFAULT NULL,
	PRIMARY KEY (`id`) USING BTREE
)
COLLATE='utf8mb4_general_ci'
ENGINE=InnoDB
;
