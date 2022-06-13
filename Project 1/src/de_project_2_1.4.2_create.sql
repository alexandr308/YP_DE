--DROP TABLE IF EXISTS de.analysis.dm_rfm_segments

CREATE TABLE de.analysis.dm_rfm_segments (
	user_id int4 NOT NULL,
	recenty int4 NOT NULL, 
	frequency int4 NOT NULL,
	monetary_value int4 NOT NULL
);