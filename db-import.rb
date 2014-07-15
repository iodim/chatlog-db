#!/usr/bin/ruby

require 'sqlite3'
require './Logfile.rb'

DBG = false

begin
	db = SQLite3::Database.open "test.db"
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
			offset INTEGER
		);
		SQL
	db.execute_batch schema
	
	user_table = (db.execute "SELECT user_name FROM users").map {|e| e = e[0]}
	channel_table = (db.execute "SELECT channel_name FROM channels").map {|e| e = e[0]}
	checksums = (db.execute "SELECT checksum FROM file_history").map {|e| e = e[0]}
	offsets = (db.execute "SELECT offset FROM file_history").map {|e| e = e[0]}
	
	file_history = Hash[checksums.zip(offsets)]

	logfile = Logfile.new "../murmur-logs/chat.log.new"

	if file_history.include? logfile.checksum
		logfile.seek(file_history[logfile.checksum], IO::SEEK_SET)
	elsif not file_history.empty?
		logfile.seek(file_history.values[-1], IO::SEEK_SET)
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
			print "\rProccessed #{progress}%"
		end
	end
rescue Exception => e
	puts "\nException occured"
	puts e	
ensure
	if file_history.include? logfile.checksum
		db.execute "UPDATE file_history SET offset=#{logfile.pos} WHERE checksum='#{logfile.checksum}'"
	else
		db.execute "INSERT INTO file_history VALUES(NULL,'#{logfile.checksum}',#{logfile.pos})"
	end
	db.close if db
	logfile.close if logfile
	puts
end 	
