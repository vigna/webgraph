#!/usr/bin/ruby

require 'bigdecimal'

n = ARGV.length

if n == 0; then
	$stderr.puts( "Usage: " +  __FILE__ + " FILES" )
	$stderr.puts( "Checks positivity of several ANF files." )
	exit
end

n.times do |i|
	t = File.new( ARGV[i] ).readlines.map{ |x| BigDecimal.new( x ) }
	for i in 0...t.size
		if t[i] < 0; then puts "#{ARGV[i]}@#{i}"; end
	end
end
