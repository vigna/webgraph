#!/usr/bin/ruby

require 'bigdecimal'

n = ARGV.length

if n == 0; then
	$stderr.puts( "Usage: " +  __FILE__ + " FILES" )
	$stderr.puts( "Checks monotonicity of several ANF files." )
	exit
end

n.times do |i|
	t = File.new( ARGV[i] ).readlines.map{ |x| BigDecimal.new( x ) }
	for d in 1...t.size
		if t[d] < t[d-1]; then puts "#{ARGV[i]}@#{d}"; end
	end
end
