package Celestial::Schema;

# set_XX
$SET = qq{
	(
	setid INT UNSIGNED NOT NULL AUTO_INCREMENT,
	spec TEXT NOT NULL,
	spec_md5 CHAR(32) NOT NULL,
	name TEXT,
	PRIMARY KEY(setid),
	UNIQUE(spec_md5)
	)
	DEFAULT CHARACTER SET utf8 COLLATE utf8_bin
};
# set_record_XX
$SET_RECORD = qq{
	(
	setid INT UNSIGNED NOT NULL,
	recordid INT UNSIGNED NOT NULL,
	PRIMARY KEY(recordid,setid)
	)
	DEFAULT CHARACTER SET utf8 COLLATE utf8_bin
};
# record_XX
$RECORD = qq{
	(
	recordid INT UNSIGNED NOT NULL AUTO_INCREMENT,
	datestamp DATETIME NOT NULL COMMENT 'Last update',
	accession DATETIME NOT NULL COMMENT 'First encountered',
	identifier TEXT NOT NULL,
	identifier_md5 CHAR(32) NOT NULL,
	status ENUM('deleted'),
	header LONGBLOB NOT NULL,
	metadata LONGBLOB,
	about LONGBLOB,
	PRIMARY KEY (recordid),
	KEY(datestamp, recordid),
	KEY(status, accession),
	UNIQUE(identifier_md5)
	)
	DEFAULT CHARACTER SET utf8 COLLATE utf8_bin
};
# fulltext_XX
$FULLTEXT = qq{
	(
	recordid INT UNSIGNED NOT NULL,
	datestamp DATETIME NOT NULL,
	url TEXT,
	mimetype VARCHAR(64),
	pronomid INT UNSIGNED,
	KEY(recordid),
	KEY(pronomid)
	)
	DEFAULT CHARACTER SET utf8 COLLATE utf8_bin
};
# pronom
$PRONOM = qq{
	(
	pronomid INT UNSIGNED NOT NULL AUTO_INCREMENT,
	puid VARCHAR(64) NOT NULL,
	name TEXT,
	PRIMARY KEY(pronomid),
	UNIQUE(puid)
	)
	DEFAULT CHARACTER SET utf8 COLLATE utf8_bin
};

1;
