#!/usr/bin/ruby

t = $stdin.readlines.map{ |x| x.to_f }
last = t[ t.size - 1 ]

puts t[0] / last
for i in 1...t.size
	puts (t[i] - t[i - 1]) / last
end
