-- This .sql file maintains the current database setup. It can be used to initialize the DB or to understand its structure. Any changes to the database should be reflected here as well.


--------------- USER MANAGEMENT ---------------
DROP TABLE IF EXISTS users;
CREATE TABLE IF NOT EXISTS users (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(128) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL
);

DROP TABLE IF EXISTS sessions;
CREATE TABLE IF NOT EXISTS sessions (
    session_id CHAR(36) PRIMARY KEY, -- UUID or similar unique token
    user_id INT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    expires_at DATETIME,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

DROP TABLE IF EXISTS user_settings;
CREATE TABLE IF NOT EXISTS user_settings (
    user_id INT PRIMARY KEY,
    transactions_load_limit INT DEFAULT 25,
    chart_balance_initial_value FLOAT DEFAULT 0,
    list_all_titles_load_limit INT DEFAULT 25,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

--------------- TRANSACTIONS ---------------
DROP TABLE IF EXISTS transactions;
CREATE TABLE IF NOT EXISTS transactions (
    transaction_id INT AUTO_INCREMENT PRIMARY KEY,
    direction ENUM('expense', 'income') NOT NULL,
    date DATE NOT NULL,
    counterparty VARCHAR(128) NOT NULL,
    notes TEXT,
    user_id INT NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

DROP TABLE IF EXISTS transaction_items;
CREATE TABLE IF NOT EXISTS transaction_items (
    item_id INT AUTO_INCREMENT PRIMARY KEY,
    transactionID INT NOT NULL,
    category VARCHAR(128) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (transactionID) REFERENCES transactions(transaction_id) ON DELETE CASCADE
);

--------------- BACKUPS ---------------
DROP TABLE IF EXISTS backups;
CREATE TABLE IF NOT EXISTS backups (
    backup_id INT AUTO_INCREMENT PRIMARY KEY,
    backup_name VARCHAR(64) NOT NULL,
    backup_direction ENUM('up', 'down') NOT NULL,
    backup_category VARCHAR(64) NOT NULL,
    peer_device VARCHAR(64) NOT NULL,
    source_path VARCHAR(128) NOT NULL,
    destination_path VARCHAR(128) NOT NULL,
    last_success DATETIME
);
-- Example how to store the schedule:
-- 0 2 * * *
-- │ │ │ │ │
-- │ │ │ │ └─ Day of the week (0 - 7) (0 and 7 are Sunday)
-- │ │ │ └─── Month (1 - 12)
-- │ │ └───── Day of the month (1 - 31)
-- │ └─────── Hour (0 - 23)
-- └───────── Minute (0 - 59)


--------------- WATCH LIST ---------------
-- Title and its children
DROP TABLE IF EXISTS titles;
CREATE TABLE IF NOT EXISTS titles (
    title_id INT AUTO_INCREMENT PRIMARY KEY,
    tmdb_id INT UNIQUE,
    imdb_id VARCHAR(10),
    type ENUM('movie', 'tv') NOT NULL,
    name VARCHAR(255),
    name_original VARCHAR(255),
    tagline VARCHAR(255),
    tmdb_vote_average DECIMAL(3,1),
    tmdb_vote_count INT,
    imdb_vote_average DECIMAL(3,1),
    imdb_vote_count INT,
    overview TEXT,
    backup_poster_url VARCHAR(255),    -- Serve as a backup for standalone and other uses
    backup_backdrop_url VARCHAR(255),  -- Serve as a backup for standalone and other uses
    movie_runtime INT DEFAULT NULL,
    release_date DATE DEFAULT NULL,
    revenue BIGINT,
    budget BIGINT,
    original_language VARCHAR(64),
    origin_country VARCHAR(64),
    awards VARCHAR(255),
    age_rating VARCHAR(10),
    trailer_key CHAR(11),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS seasons;
CREATE TABLE IF NOT EXISTS seasons (
    season_id INT AUTO_INCREMENT PRIMARY KEY,
    title_id INT NOT NULL,
    season_number INT NOT NULL,
    season_name VARCHAR(255),
    tmdb_vote_average DECIMAL(3,1),
    tmdb_vote_count INT,
    episode_count SMALLINT,
    overview TEXT,
    backup_poster_url VARCHAR(255),    
    -- For the air date use season[0].air_date
    FOREIGN KEY (title_id) REFERENCES titles(title_id) ON DELETE CASCADE,
    UNIQUE(title_id, season_number), -- Add this unique constraint
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS episodes;
CREATE TABLE IF NOT EXISTS episodes (
    episode_id INT AUTO_INCREMENT PRIMARY KEY,
    season_id INT NOT NULL,  
    title_id INT NOT NULL,  
    episode_number INT NOT NULL,
    episode_name VARCHAR(255),
    tmdb_vote_average DECIMAL(3,1),
    tmdb_vote_count INT,
    overview TEXT,
    backup_still_url VARCHAR(255),
    air_date DATE DEFAULT NULL,
    runtime INT DEFAULT NULL,
    FOREIGN KEY (season_id) REFERENCES seasons(season_id) ON DELETE CASCADE,
    FOREIGN KEY (title_id) REFERENCES titles(title_id) ON DELETE CASCADE,
    UNIQUE(season_id, episode_number), -- Add this unique constraint
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- User details
DROP TABLE IF EXISTS user_title_details;
CREATE TABLE IF NOT EXISTS user_title_details (
    user_id INT NOT NULL,
    title_id INT NOT NULL,
    watch_count INT DEFAULT 0,
    notes TEXT DEFAULT NULL,
    favourite BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (user_id, title_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (title_id) REFERENCES titles(title_id) ON DELETE CASCADE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS user_episode_details;
CREATE TABLE IF NOT EXISTS user_episode_details (
    user_id INT NOT NULL,
    episode_id INT NOT NULL,
    watch_count INT DEFAULT 0,  
    notes TEXT DEFAULT NULL,  
    PRIMARY KEY (user_id, episode_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (episode_id) REFERENCES episodes(episode_id) ON DELETE CASCADE
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Genres
DROP TABLE IF EXISTS genres;
CREATE TABLE IF NOT EXISTS genres (
    genre_id INT AUTO_INCREMENT PRIMARY KEY,
    tmdb_genre_id INT,
    genre_name VARCHAR(255) NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS title_genres;
CREATE TABLE IF NOT EXISTS title_genres (
    title_id INT NOT NULL,
    genre_id INT NOT NULL,
    PRIMARY KEY (title_id, genre_id),
    FOREIGN KEY (title_id) REFERENCES titles(title_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE CASCADE
);

-- Collections
DROP TABLE IF EXISTS user_collection;
CREATE TABLE IF NOT EXISTS user_collection (
    collection_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    parent_collection_id INT DEFAULT NULL,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (parent_collection_id) REFERENCES user_collection(collection_id) ON DELETE CASCADE
);

DROP TABLE IF EXISTS collection_title;
CREATE TABLE IF NOT EXISTS collection_title (
    collection_id INT NOT NULL,
    title_id INT NOT NULL,
    PRIMARY KEY (title_id, collection_id),
    FOREIGN KEY (collection_id) REFERENCES user_collection(collection_id) ON DELETE CASCADE,
    FOREIGN KEY (title_id) REFERENCES titles(title_id) ON DELETE CASCADE
);


--------------- INDEXES ---------------

-- Just ran this and left it be
CREATE INDEX idx_user_id ON transactions(user_id, date);


-- Decided to not mess with these for now, but here are some that I messed with
DROP INDEX idx_user_id ON transactions;

SHOW INDEX FROM transactions;
SHOW INDEX FROM transaction_items;
EXPLAIN SELECT * 
FROM transactions 
WHERE user_id = 2;

