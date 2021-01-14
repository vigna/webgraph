#!/usr/bin/ruby

@t = $stdin.readlines.map{ |x| x.to_i }
d = []
@t.size.times do |i| d <<= i; end

d.sort!{|x,y| @t[y] <=> @t[x]}

def rankdeg n
	(@t.size-1).downto(0) do |i|
		n -= @t[i]
		if n < 0; then return i; end
	end
	throw :cantHappen
end

@cache = []

def sum k
	if k < @cache.size; then return @cache[k]; end
	s = 0
	n = 0
	l = rankdeg(k)
	(@t.size-1).downto(0) do |i|
		s += @t[i] * i
		n += @t[i]
		if n > l; then return s - ( n - l ) * i; end
	end
	throw :cantHappen
end	

#for i in 0..6000
#	@cache[i] = sum(i)
#end

def stupidsum k
	dk = rankdeg(k)
	s = 0
	for i in 0...dk
		s += rankdeg(i)
	end
	s
end	

n = 0
@t.each do |x| n += x; end
nn = n * n

puts "Nodes: #{n}"

r = 721000000*0.9991
r *= r

D = 6000

# First step: degree one neighbours (arcs)

a = 0
@t.size.times.each do |i| a += i * @t[i]; end

puts "Arcs: #{a} (#{a.to_f / nn })"

puts "Bound maxdeg: #{(a + 2 * 6000 * a + 3 * ( r - ( a + 6000 * a )  ) ).to_f / r}"

s = 0
@t.size.times.each do |i| s += i * i * @t[i]; end

puts "Squares: #{s}  (#{s.to_f / r })"
puts "Bound: #{(a + 2 * s + 3 * ( r - ( a + s ) ) ).to_f / r}"

k = @t.size
sd = 0
b = 0

loop do
	k -= 1
	b += @t[k] * k
	sd += @t[k]
	if sd * sd > a; then break; end
end

b *= b

puts "Matrix bound: #{b}  (#{b.to_f / r })"
puts "Bound: #{(a + 2 * s + 3 * b + 4 * ( r - ( a + s + b ) ) ).to_f / r}"

k = @t.size
sd = 0
b = 0

loop do
	k -= 1
	sd += k * @t[k]
	b += k * k * @t[k]
	if sd > s / ( @t.size - 1 ); then break; end
end

b *= ( @t.size - 1 )

puts "Matrix 2 bound: #{b}  (#{b.to_f / r })"
puts "Bound: #{(a + 2 * s + 3 * b + 4 * ( r - ( a + s + b ) ) ).to_f / r}"

#10000.times do |x|
#	p x
#	p sum(x)
#	p stupidsum(x)
#	if sum(x) != stupidsum(x); then throw :doesntWork; end
#end


#l = 0
#sd = 0
#b = 0
#
#loop do
#	ss = sum(l)
#	sd += ss
#	b += rankdeg(l) * ss
#	l += 1
#	#puts "#{l}: #{b} #{b.to_f / nn} #{sd} <> #{s}"
#	if sd > s; then break; end
#	if l % 1000 == 0; then puts "#{l}: #{sd} <= #{s} #{100. * sd.to_f / s}"; puts "Bound: #{(a + 2 * s + 3 * b + 4 * ( nn - ( a + s + b ) ) ).to_f / nn}"; end
#end
#
#puts "Matrix 3 bound: #{b}  (#{b.to_f / nn })"
#puts "Bound: #{(a + 2 * s + 3 * b + 4 * ( nn - ( a + s + b ) ) ).to_f / nn}"
