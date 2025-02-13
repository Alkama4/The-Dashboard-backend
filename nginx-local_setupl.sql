
--------------- USER MANAGEMENT ---------------
DROP TABLE IF EXISTS users;
CREATE TABLE IF NOT EXISTS users (
    userID INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(128) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL
);

DROP TABLE IF EXISTS sessions;
CREATE TABLE IF NOT EXISTS sessions (
    sessionID CHAR(36) PRIMARY KEY, -- UUID or similar unique token
    userID INT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    expires_at DATETIME,
    FOREIGN KEY (userID) REFERENCES users(userID) ON DELETE CASCADE
);

--------------- TRANSACTIONS ---------------
DROP TABLE IF EXISTS transactions;
CREATE TABLE IF NOT EXISTS transactions (
    transactionID INT AUTO_INCREMENT PRIMARY KEY,
    direction ENUM('expense', 'income') NOT NULL,
    date DATE NOT NULL,
    counterparty VARCHAR(128) NOT NULL,
    notes TEXT,
    userID INT NOT NULL,
    FOREIGN KEY (userID) REFERENCES users(userID) ON DELETE CASCADE
);

DROP TABLE IF EXISTS transaction_items;
CREATE TABLE IF NOT EXISTS transaction_items (
    itemID INT AUTO_INCREMENT PRIMARY KEY,
    transactionID INT NOT NULL,
    category VARCHAR(128) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (transactionID) REFERENCES transactions(transactionID) ON DELETE CASCADE
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
    title_name VARCHAR(255) NOT NULL,
    title_name_original VARCHAR(255),
    tagline VARCHAR(255),
    vote_average DECIMAL(3,1),
    vote_count INT,
    overview TEXT,
    poster_url VARCHAR(255),    -- Serve as a backup I guess for now
    backdrop_url VARCHAR(255),  -- Serve as a backup I guess for now
    movie_runtime INT DEFAULT NULL,
    release_date DATE DEFAULT NULL,
    original_language VARCHAR(10),
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
    vote_average DECIMAL(3,1),
    vote_count INT,
    episode_count SMALLINT,
    overview TEXT,
    poster_url VARCHAR(255),    
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
    vote_average DECIMAL(3,1),
    vote_count INT,
    overview TEXT,
    still_url VARCHAR(255),
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
    userID INT NOT NULL,
    title_id INT NOT NULL,
    watch_count INT DEFAULT 0,
    notes TEXT DEFAULT NULL,
    favourite BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (userID, title_id),
    FOREIGN KEY (userID) REFERENCES users(userID) ON DELETE CASCADE,
    FOREIGN KEY (title_id) REFERENCES titles(title_id) ON DELETE CASCADE,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS user_episode_details;
CREATE TABLE IF NOT EXISTS user_episode_details (
    userID INT NOT NULL,
    episode_id INT NOT NULL,
    watch_count INT DEFAULT 0,  
    notes TEXT DEFAULT NULL,  
    PRIMARY KEY (userID, episode_id),
    FOREIGN KEY (userID) REFERENCES users(userID) ON DELETE CASCADE,
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



--------------- SERVER LOGS ---------------
DROP TABLE IF EXISTS server_resource_logs;
CREATE TABLE server_resource_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    cpu_temperature FLOAT NOT NULL,
    ram_usage FLOAT NOT NULL,
    cpu_usage FLOAT NOT NULL,
    disk_usage FLOAT NOT NULL,
    system_load FLOAT NOT NULL,
    network_sent_bytes BIGINT NOT NULL,
    network_recv_bytes BIGINT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



--------------- INDEXES ---------------

-- Just ran this and left it be
CREATE INDEX idx_user_id ON transactions(userID, date);


-- Decided to not mess with these for now but here they are
DROP INDEX idx_user_id ON transactions;

SHOW INDEX FROM transactions;
SHOW INDEX FROM transaction_items;
EXPLAIN SELECT * 
FROM transactions 
WHERE userID = 2;



--------------- DEFAULT VALUES ---------------

-- Insert guest user (hardcoding userID as 1)
INSERT INTO users (userID, username, password) 
VALUES 
(1, 'guest', '-'),
(2, 'Aleksi', 'salasana');

-- Example transactions with hardcoded guest userID (1)
INSERT INTO transactions (transactionID, direction, date, counterparty, notes, userID) 
VALUES
(1, 'expense', '2023-08-24', 'Cotton Club', 'Hernekeittoa', 1),
(2, 'income', '2023-12-28', 'Kela', '', 1),
(3, 'expense', '2023-12-25', 'K-Citymarket', 'Jotaki mikä nyt voitas luokitella yleisen elämisen luokkaan', 1),
(4, 'expense', '2023-12-28', 'Minimani', 'safkaa', 1),
(5, 'expense', '2023-12-29', 'Cotton Club', 'Hyvää ruokaa', 1),
(6, 'expense', '2023-12-31', 'S-Market', 'Mässy pussi', 1),
(7, 'expense', '2024-01-01', 'Minimani', 'Ruokaa ja palaa', 1),
(8, 'expense', '2023-12-26', 'Supermarket', 'Vähään kaikkea kulutustavarasta ruoasta herkkuihin.', 1),
(9, 'expense', '2024-01-03', 'K-Citymarket', 'Jotaki safkaa', 1),
(10, 'expense', '2024-12-24', 'Parturi Hannele-Kallio', 'Jotaki safkaa', 1);

-- Example transaction items associated with the above transactions
INSERT INTO transaction_items (itemID, transactionID, category, amount) VALUES
(1, 1, 'Opiskelija lounas', 2.90),
(2, 2, 'Asumistuki', 99.99),
(3, 2, 'Opintotuki', 80.00),
(4, 3, 'Yleinen eläminen', 8.75),
(5, 4, 'Ruokaostokset', 20.24),
(6, 5, 'Kulutustavara', 12.46),
(7, 6, 'Herkut', 1.99),
(8, 6, 'Ruokaostokset', 9.34),
(9, 7, 'Ruokaostokset', 15.96),
(10, 8, 'Sekalainen', 25.00),
(11, 8, 'Ruokaostokset', 14.23),
(12, 8, 'Herkut', 6.39),
(13, 9, 'Ruokaostokset', 4.84),
(14, 10, 'Parturi', 900.67);

-- Initial backups setup
-- NOT UP TO DATE!!
INSERT INTO backups (name, source_location, source_path, destination_location, destination_path, last_success, schedule)
VALUES 
    ('Black-Box Backup', 'Pinas', '\\\\192.168.0.2\\PiNas', 'Black-Box', 'D:\\_PiNas_automated_backup', NULL, '0 0 * * *'),
    ('Vue projects', 'Black-Box', 'C:\\Users\\aleks\\Documents\\_Vue-projects\\', 'PiNas', '\\\\192.168.0.2\\PiNas\\Tiedostot\\Koodit\\Vue\\', NULL, '0 0 * * *'),
    ('Satisfactory', 'Black-Box', 'C:\\Users\\aleks\\Documents\\My Games\\FactoryGame\\Screenshots\\', 'PiNas', '\\\\192.168.0.2\\PiNas\\Media\\Muistot\\Game clips\\Satisfactory\\', NULL, '0 0 * * *'),
    ('Cyberpunk', 'Black-Box', 'C:\\Users\\aleks\\Pictures\\Cyberpunk 2077\\', 'PiNas', '\\\\192.168.0.2\\PiNas\\Media\\Muistot\\Game clips\\Cyberpunk 2077\\', NULL, '0 0 * * *'),
    ('Noita', 'Black-Box', 'C:\\Users\\aleks\\AppData\\LocalLow\\Nolla_Games_Noita\\save_rec\\screenshots_animated\\', 'PiNas', '\\\\192.168.0.2\\PiNas\\Media\\Muistot\\Game clips\\Noita\\', NULL, '0 0 * * *');

-- Single
INSERT INTO backups (name, source_location, source_path, destination_location, destination_path, last_success, schedule)
VALUES ('Mysql', 'local-nginx-mysql', 'nginxLocal', 'PiNas', '\\PiNas\\Tiedostot\\Backups\\Webserver\\mysql_auto', NULL, '0 4 * * *');
INSERT INTO backups (name, source_location, source_path, destination_location, destination_path, last_success, schedule)
VALUES ('Fastapi', 'PiBox', '\\srv\\www\\local-nginx-fastapi\\', 'PiNas', '\\PiNas\\Tiedostot\\Backups\\Webserver\\fastapi_auto', NULL, '0 4 * * *');


