#!/usr/bin/ruby

t = $stdin.readlines.map{ |x| x.to_f }
last = t[ t.size - 1 ]

for i in 0...t.size
	puts t[i] / last
end
