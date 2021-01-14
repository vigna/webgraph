#!/usr/bin/ruby

require 'bigdecimal'

n = ARGV.length - 1

if n <= 0; then
	$stderr.puts( "Usage: jackknife.rb FILES (EXACT|-)" )
	$stderr.puts( "Computes jackknife estimates and optionally estimates the error using an exact ANF." )
	exit
end

if ARGV[-1] != "-"; then exact = File.new( ARGV[-1] ).readlines.map{ |x| x.to_f }; end

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

def dist v
	if v == nil; then return nil; end
	s = 0
	v.each_index do |d| s += v[d]; end
	return v.size - s / v[-1]
end

def hdiam v
	if v == nil; then return nil; end
	s = 0
	for d in 1...v.size
		s += ( v[d] - v[d-1] ) / d
	end
	return v[0] * ( v[0] - 1 ) / s
end

def spid v
	if v == nil; then return nil; end
	s = 0
	ss = 0
	for d in 1...v.size
		t = d * (v[d] - v[d-1])
		s += t
		ss += d * t
	end
	return ss / s - s / v[-1]
end

def jvariance a
	ma = mean(a)
	c = 0
	a.each_index do |i|
		c += (a[i] - ma)*(a[i] - ma)
	end
	(a.size-1) * c / a.size
end

def formaterror v,e
	"#{( v - e ) / e}"
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
nodes = exact ? exact[0] : nf[0][0]

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

lo_dist = []
lo_hdiam = []
lo_spid = []

biased_dist = mean(sum_dist) / reachable
biased_hdiam = nodes * ( nodes - 1 ) / mean(sum_invdist)
biased_spid = mean(sum_sqdist) / mean(sum_dist) - mean(sum_dist) / reachable

n.times do |i|
	sd = sum_dist.dup; sd.delete_at(i)
	ssqd = sum_sqdist.dup; ssqd.delete_at(i)
	si = sum_invdist.dup; si.delete_at(i)
	l = last.dup; l.delete_at(i)

	lo_dist <<= mean(sd) / mean(l)
	lo_hdiam <<= nodes * ( nodes - 1 ) / mean(si)
	lo_spid <<= mean(ssqd) / mean(sd) - mean(sd) / mean(l)
end

puts "reachablepairs=#{mean(last)}"
puts "reachablepairsstderr=#{Math.sqrt(variance(last)/n)}"
if exact; then puts "reachablepairserr=#{formaterror(n*biased_dist - (n-1)*mean(lo_dist),exact[exact.size-1])}"; end 
puts "reachablepairsperc=#{100*mean(last)/(nodes*nodes)}"
puts "reachablepairspercstderr=#{100*Math.sqrt(variance(last)/n)/(nodes*nodes)}"
puts "averagedistance=#{n*biased_dist - (n-1)*mean(lo_dist)}"
if exact; then puts "averagedistanceerr=#{formaterror(n*biased_dist - (n-1)*mean(lo_dist),dist(exact))}"; end 
puts "averagedistancestderr=#{Math.sqrt(jvariance(lo_dist))}"
puts "harmonicdiameter=#{n*biased_hdiam - (n-1)*mean(lo_hdiam)}"
if exact; then puts "harmonicdiametererr=#{formaterror(n*biased_hdiam - (n-1)*mean(lo_hdiam),hdiam(exact))}"; end
puts "harmonicdiameterstderr=#{Math.sqrt(jvariance(lo_hdiam))}"
puts "spid=#{n*biased_spid - (n-1)*mean(lo_spid)}"
if exact; then puts "spid=#{formaterror(n*biased_spid - (n-1)*mean(lo_spid),spid(exact))}"; end
puts "spidstderr=#{Math.sqrt(jvariance(lo_spid))}"
