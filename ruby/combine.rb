#!/usr/bin/ruby

require 'bigdecimal'

n = ARGV.length

if n == 0; then
	$stderr.puts( "Usage: combine.rb FILES" )
	$stderr.puts( "Combines several ANF files into a single averaged file, enforcing monotonicity." )
	exit
end

nf = []
max = -1

n.times do |i|
	t = File.new( ARGV[i] ).readlines.map{ |x| BigDecimal.new( x ) }
	if t.size > max; then max = t.size; end
	nf <<= t
end

last = BigDecimal.new("0")

max.times do |i|
	avg = BigDecimal.new("0")
	nf.size.times do |j|
		l = nf[j].size
		if i < l; then v = nf[j][i]; else v = nf[j][l-1]; end
		avg += v
	end

	avg /= BigDecimal.new( nf.size.to_s )
	if avg < last; then avg = last; end
	puts avg.to_s
	last = avg
end
