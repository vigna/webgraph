#!/usr/bin/ruby

n = ARGV.length

if n == 0; then
	$stderr.puts( "Usage: ed.rb cdf cdfsd" )
	$stderr.puts( "Prints a confidence interval for the effective diameter." )
	exit
end

cdf = File.new( ARGV[0] ).readlines.map{ |x| x.to_f }
cdfsd = File.new( ARGV[1] ).readlines.map{ |x| x.to_f }

k = 0
for i in 0...cdf.size
	if cdf[i]/(1-3*cdfsd[i]) >= 0.9; then break; end
	k += 1
end

j = 0
for i in 0...cdf.size
	if cdf[i]/(1+3*cdfsd[i]) >= 0.9; then break; end
	j += 1
end

puts "[#{k}..#{j}]"

$stderr.puts
$stderr.puts cdf[k-1]/(1-3*cdfsd[k-1])
$stderr.puts cdf[k]/(1-3*cdfsd[k])
$stderr.puts cdf[k+1]/(1-3*cdfsd[k+1])
$stderr.puts
$stderr.puts cdf[j-1]/(1+3*cdfsd[j-1])
$stderr.puts cdf[j]/(1+3*cdfsd[j])
$stderr.puts cdf[j+1]/(1+3*cdfsd[j+1])
