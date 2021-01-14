#!/usr/bin/ruby

require 'bigdecimal'

n = ARGV.length - 1

exact = File.new( ARGV[-1] ).readlines.map{ |x| x.to_f }

if n == 0; then
	$stderr.puts( "Usage: jackknife.rb FILES EXACT" )
	$stderr.puts( "Computes jackknife estimates and estimates the error using an exact ANF." )
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

def j2 n, biased, sum_lo, sum_lo2
	0.5 * (n*n*biased - 2 * (n - 1) * (n - 1) * sum_lo / n + (n - 2) * (n - 2) * sum_lo2 / (n * (n - 1)))
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

sum_lo_dist = 0
sum_lo_hdiam = 0
sum_lo_spid = 0

sum_lo2_dist = 0
sum_lo2_hdiam = 0
sum_lo2_spid = 0

n.times do |i|
	sd = sum_dist.dup; sd.delete_at(i)
	ssqd = sum_sqdist.dup; ssqd.delete_at(i)
	si = sum_invdist.dup; si.delete_at(i)
	l = last.dup; l.delete_at(i)

	n.times do |j|
		if i != j; then
			sd2 = sd.dup; sd2.delete_at(j)
			ssqd2 = ssqd.dup; ssqd2.delete_at(j)
			si2 = si.dup; si2.delete_at(j)
			l2 = l.dup; l2.delete_at(j)
			
			sum_lo2_dist += mean(sd2) / mean(l2)
			sum_lo2_hdiam += nodes * ( nodes - 1 ) / mean(si2)
			sum_lo2_spid += mean(ssqd2) / mean(sd2) - mean(sd2) / mean(l2)
		else
			sum_lo_dist += mean(sd) / mean(l)
			sum_lo_hdiam += nodes * ( nodes - 1 ) / mean(si)
			sum_lo_spid += mean(ssqd) / mean(sd) - mean(sd) / mean(l)
		end
	end

	distpv <<= n * biased_dist - (n - 1) * ( mean(sd) / mean(l) )
	hdiampv <<= n * biased_hdiam - (n - 1) * ( nodes * ( nodes - 1 ) / mean(si) )
	spidpv <<= n * biased_spid - (n - 1) * ( mean(ssqd) / mean(sd) - mean(sd) / mean(l) )
end

puts "Old average distance (mean of the ratios): #{format(mean(sample_avgdist),dist(exact))} ±#{Math.sqrt(variance(sample_avgdist))}"
puts "Average distance as ratio of the means: #{format(biased_dist,dist(exact))}"
puts "Jackknifed average distance: #{format(mean(distpv),dist(exact))} ±#{Math.sqrt(variance(distpv)/n)}"
puts "Jackknifed² average distance: #{format(j2(n,biased_dist,sum_lo_dist,sum_lo2_dist),dist(exact))} ±#{Math.sqrt(variance(distpv)/n)}"
puts
puts "Old average harmonic diameter (mean of the inverses): #{format(mean(sample_hdiam),hdiam(exact))} ±#{Math.sqrt(variance(sample_hdiam))}"
puts "Harmonic diameter as reciprocal of the mean: #{format(biased_hdiam,hdiam(exact))}"
puts "Jackknifed harmonic diameter: #{format(mean(hdiampv),hdiam(exact))} ±#{Math.sqrt(variance(hdiampv)/n)}"
puts "Jackknifed² harmonic diameter: #{format(j2(n,biased_hdiam,sum_lo_hdiam,sum_lo2_hdiam),hdiam(exact))} ±#{Math.sqrt(variance(hdiampv)/n)}"
puts
puts "Old spid (mean of the values): #{format(mean(sample_spid),spid(exact))} ±#{Math.sqrt(variance(sample_spid))}"
puts "spid evaluated on the mean: #{format(biased_spid,spid(exact))}"
puts "Jackknifed spid: #{format(mean(spidpv),spid(exact))} ±#{Math.sqrt(variance(spidpv)/n)}"
puts "Jackknifed² spid: #{format(j2(n,biased_spid,sum_lo_spid,sum_lo2_spid),spid(exact))} ±#{Math.sqrt(variance(spidpv)/n)}"
