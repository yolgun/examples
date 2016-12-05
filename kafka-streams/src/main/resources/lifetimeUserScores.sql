drop database if exists firefly_stats;
create schema firefly_stats;
CREATE TABLE firefly_stats.`lifetime_scores` (
  `userID` int(11) NOT NULL,
  `scoreType` varchar(256) NOT NULL,
  `value` int(11) NOT NULL,
  PRIMARY KEY (`scoreType`,`userID`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
