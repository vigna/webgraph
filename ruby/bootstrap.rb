#!/usr/bin/ruby

require 'bigdecimal'

n = ARGV.length - 1

exact = File.new( ARGV[-1] ).readlines.map{ |x| x.to_f }

if n == 0; then
	$stderr.puts( "Usage: bootstrap.rb FILES EXACT" )
	$stderr.puts( "Computes bootstrap estimates and estimates the error using an exact ANF." )
	exit
end

def mean a
	a.inject(0) { |s,v| s += v } / a.size
end

def variance a
	ma = mean(a)
	c = 0
	a.each_index do |i|
		c += (a[i] - ma)*(a[i] - ma)
	end
	c / (a.size-1)
end

def stddev a
	Math.sqrt(variance(a))
end

def dist v
	s = 0
	v.each_index do |d| s += v[d]; end
	return v.size - s / v[-1]
end

def hdiam v
	s = 0
	for d in 1...v.size
		s += ( v[d] - v[d-1] ) / d
	end
	return v[0] * ( v[0] - 1 ) / s
end

def spid v
	s = 0
	ss = 0
	for d in 1...v.size
		t = d * (v[d] - v[d-1])
		s += t
		ss += d * t
	end
	return ss / s - s / v[-1]
end

def format v,e
	sprintf("%.3f (%.3f%%)", v, 100.0*( v - e ) / e)
end

nf = []
last = []
max = -1

n.times do |i|
	t = File.new( ARGV[i] ).readlines.map{ |x| x.to_f }
	if t.size > max; then max = t.size; end
	last <<= t[-1]
	nf <<= t
end

# Extend everything to max values.

n.times do |i|
	while nf[i].size < max
		nf[i] <<= nf[i][-1]
	end
end

reachable = mean(last)
nodes = exact[0]

sum_dist = []
sum_sqdist = []
sum_invdist = []
sample_avgdist = []
sample_hdiam = []
sample_spid = []

# Compute sums of the first max values of the neighbourhood function, possibly divided by the last value.

n.times do |i|
	sum_dist[i] = 0
	sum_invdist[i] = 0
	sum_sqdist[i] = 0
	for d in 1...max
		sum_dist[i] += (nf[i][d] - nf[i][d-1])*d
		sum_sqdist[i] += (nf[i][d] - nf[i][d-1])*d*d
		sum_invdist[i] += (nf[i][d] - nf[i][d-1])/d
	end

	# Computes values of interest for each sample.
	sample_avgdist <<= sum_dist[i] / last[i]
	sample_hdiam <<= nodes * (nodes - 1) / sum_invdist[i]
	sample_spid <<= sum_sqdist[i] / sum_dist[i] - sum_dist[i] / last[i]
end

# Compute pseudovalues.

distpv = []
hdiampv = []
spidpv = []

biased_dist = mean(sum_dist) / reachable
biased_hdiam = nodes * ( nodes - 1 ) / mean(sum_invdist)
biased_spid = mean(sum_sqdist) / mean(sum_dist) - mean(sum_dist) / reachable

srand(0)

samples = [1000, (n*Math.log(n)*Math.log(n)).to_i].max

puts "Using #{samples} samples..."

samples.times do |i|
	snf = nf[rand(n)].dup
	(n-1).times do
		s = rand(n)
		max.times do |d| snf[d] += nf[s][d]; end
	end
	snf.map! { |x| x /= n }

	distpv <<= dist(snf)
	hdiampv <<= hdiam(snf)
	spidpv <<= spid(snf)
end

puts "Old average distance (mean of the ratios): #{format(mean(sample_avgdist),dist(exact))} ±#{stddev(sample_avgdist)}"
puts "Average distance as ratio of the means: #{format(biased_dist,dist(exact))}"
puts "Boostraped average distance: #{format(mean(distpv),dist(exact))} ±#{stddev(distpv)}"
puts
puts "Old average harmonic diameter (mean of the inverses): #{format(mean(sample_hdiam),hdiam(exact))} ±#{stddev(sample_hdiam)}"
puts "Harmonic diameter as reciprocal of the mean: #{format(biased_hdiam,hdiam(exact))}"
puts "Boostraped harmonic diameter: #{format(mean(hdiampv),hdiam(exact))} ±#{stddev(hdiampv)}"
puts
puts "Old spid (mean of the values): #{format(mean(sample_spid),spid(exact))} ±#{stddev(sample_spid)}"
puts "spid evaluated on the mean: #{format(biased_spid,spid(exact))}"
puts "Boostraped spid: #{format(mean(spidpv),spid(exact))} ±#{stddev(spidpv)}"
