CREATE TABLE users(
	userid INT NOT NULL UNIQUE,
	name TEXT NOT NULL,
	PRIMARY KEY(userid)
);

CREATE TABLE movies(
	movieid INT NOT NULL UNIQUE,
	title TEXT NOT NULL,
	PRIMARY KEY(movieid)
);

CREATE TABLE taginfo(
	tagid INT NOT NULL UNIQUE,
	content TEXT NOT NULL,
	PRIMARY KEY(tagid)
);

CREATE TABLE genres(
	genreid INT NOT NULL,
	name TEXT NOT NULL,
	PRIMARY KEY(genreid)
);

CREATE TABLE ratings(
	userid INT NOT NULL,
	movieid INT NOT NULL,
	rating NUMERIC NOT NULL CHECK(rating >=0 AND rating <=5),
	timestamp BIGINT NOT NULL,
	FOREIGN KEY (userid)
		REFERENCES users,
	FOREIGN KEY (movieid)
		REFERENCES movies,
	PRIMARY KEY (userid, movieid, rating)
);

CREATE TABLE tags(
	userid INT NOT NULL,
	movieid INT NOT NULL,
	tagid INT NOT NULL,
	timestamp BIGINT,
	FOREIGN KEY(userid)
		REFERENCES users,
	FOREIGN KEY(movieid)
		REFERENCES movies,
	FOREIGN KEY(tagid)
		REFERENCES taginfo
);

CREATE TABLE hasagenre(
	movieid INT REFERENCES movies,
	genreid INT REFERENCES genres,
	PRIMARY KEY (movieid, genreid)
);