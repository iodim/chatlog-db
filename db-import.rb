#!/usr/bin/ruby

require 'sqlite3'
require './Logfile.rb'

DBG = false

log_path = "chat.log"
db_path = "chat.db"

ARGV.each_with_index do |option, i|
	case option
	when "-v", "--verbose"
		DBG = true
	when "-l", "--log"
		log_path = ARGV[i+1]
	when "-d", "--db"
		db_path = ARGV[i+1]
	when "-h", "--help"
		puts <<-EOF
Usage: db-import [OPTION]...
  -v, --verbose\t\t\tEnable verbose output
  -l [FILE], --log [FILE]\tSpecify log file (must exist)
  -d [FILE], --db [FILE]\tSpecify database file
  -h, --help\t\t\tDisplay this message
		EOF
	end
end

begin
	db = SQLite3::Database.open db_path
	schema = <<-SQL
		PRAGMA foreign_keys = ON;
		PRAGMA synchronous = OFF;
		PRAGMA temp_store = MEMORY;

		CREATE TABLE IF NOT EXISTS users (
			user_id INTEGER PRIMARY KEY,
			user_name TEXT
		);
		
		CREATE TABLE IF NOT EXISTS channels (
			channel_id INTEGER PRIMARY KEY,
			channel_name TEXT
		);

		CREATE TABLE IF NOT EXISTS log_entries (
			entry_id INTEGER PRIMARY KEY,
			user_id INTEGER,
			channel_id INTEGER,
			day INTEGER,
			month INTEGER,
			year INTEGER,
			time TEXT,
			message TEXT,
			FOREIGN KEY (user_id) REFERENCES users(user_id),
			FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
		);

		CREATE TABLE IF NOT EXISTS file_history (
			revision_id INTEGER PRIMARY KEY,
			checksum TEXT,
			start_pos INTEGER,
			end_pos INTEGER,
			size INTEGER
		);
		SQL
	db.execute_batch schema
	
	user_table = (db.execute "SELECT user_name FROM users").map {|e| e = e[0]}
	channel_table = (db.execute "SELECT channel_name FROM channels").map {|e| e = e[0]}
	checksum_table = (db.execute "SELECT checksum FROM file_history").map {|e| e = e[0]}
	#start_pos_table = (db.execute "SELECT start_pos FROM file_history").map {|e| e = e[0]}
	end_pos_table = (db.execute "SELECT end_pos FROM file_history").map {|e| e = e[0]}
	#size_table = (db.execute "SELECT size FROM file_history").map {|e| e = e[0]}
	
	logfile = Logfile.new log_path

	start_pos = 0
	update_file_history = true

	unless checksum_table.empty?
		start_pos = end_pos_table[-1]
		if start_pos < logfile.size
			logfile.seek(start_pos, IO::SEEK_SET)
			if checksum_table[-1] == logfile.checksum
				puts "Resuming..."
			else
				puts "Log file changed. Continuing from last imported line..."
			end
		elsif start_pos == logfile.size
			puts "Log already imported. Nothing to do."
			logfile.seek(start_pos, IO::SEEK_SET)
		else
			update_file_history = false
			raise "Resume position greater than log size. Is this an older log file?"
		end
	end

	logfile.each_line do |line|
		match = line.match(/^(?:(\d{2})(?:-))(?:(\d{2})(?:-))(\d{4})\s((?:\d{2}:){2}\d{2})\s\[((?:\w|\s|\d)+)\]\s((?:\w|\d|\s|\[|\])+):((?:\n|.)*)$/)
		if match
			dd, mm, yyyy, time, channel_name, user_name, message = match.captures
			
			message.chomp!.gsub!(/\'/, '&#39;')
			
			if user_table.include? user_name
				user_id = user_table.index(user_name) + 1
			else
				user_table << user_name
				puts "INSERT INTO users VALUES(NULL,'#{user_name}')" if DBG
				db.execute "INSERT INTO users VALUES(NULL,'#{user_name}')"
				user_id = user_table.size
			end

			if channel_table.include? channel_name
				channel_id = channel_table.index(channel_name) + 1
			else
				channel_table << channel_name
				puts "INSERT INTO channels VALUES(NULL,'#{channel_name}')" if DBG
				db.execute "INSERT INTO channels VALUES(NULL,'#{channel_name}')"
				channel_id = channel_table.size
			end

			puts "INSERT INTO log_entries VALUES(NULL,#{user_id},#{channel_id},#{dd},#{mm},#{yyyy},'#{time}','#{message}')" if DBG
			db.execute "INSERT INTO log_entries VALUES(NULL,#{user_id},#{channel_id},#{dd},#{mm},#{yyyy},'#{time}','#{message}')"
		end
		
		unless DBG
			progress = (logfile.pos/logfile.size.to_f * 100).to_i
			print "Proccessed #{progress}%\r"
		end
	end
	puts "Log imported successfully."
rescue Exception => e
	puts e
ensure
	if logfile
		if update_file_history
			puts "INSERT INTO file_history VALUES(NULL,'#{logfile.checksum}',#{start_pos},#{logfile.pos},#{logfile.size})" if DBG
			db.execute "INSERT INTO file_history VALUES(NULL,'#{logfile.checksum}',#{start_pos},#{logfile.pos},#{logfile.size})"
		end
		logfile.close
	end
	db.close if db
end 	
