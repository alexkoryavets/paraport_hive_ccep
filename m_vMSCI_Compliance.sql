drop table if exists `ccep`.`m_vMSCI_Compliance`;

CREATE TABLE `ccep`.`m_vMSCI_Compliance` (
	`security_tkn_id` int,
	`company_id` string,
	`entity_name` string,
	`issuecolumnname` string,
	`issuevalue` string,
	`issuevaluefloat` float,
	`issue_source` string,
	`datekey` int,
	`quarterenddate` string)
PARTITIONED BY (`reportingperiod` string);

insert into `ccep`.`m_vMSCI_Compliance` partition (reportingperiod)
SELECT
	`security_tkn_id`,
	`company_id`,
	`entity_name`,
	`issuecolumnname`,
	`issuevalue`,
	`issuevaluefloat`,
	`issue_source`,
	`datekey`,
	`quarterenddate`,
	`reportingperiod`
FROM `ccep`.`vMSCI_Compliance`;