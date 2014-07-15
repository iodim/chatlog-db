#!/usr/bin/ruby

require 'digest'

class Logfile < File
	
	def initialize(file_path)
		@checksum = Digest::MD5.hexdigest(File.read(file_path))
		super(file_path, 'r')
	end

	def checksum
		@checksum
	end

end