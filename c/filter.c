/* Filters a list of pairs c:n interpreting them as counts (c) for number (n).
   Normalises and inverts. */

#include <stdio.h>

int C[100000000];

int main(void) {
	int i, c, n;
	double tot = 0;
	int offset = 1;
	while( scanf( "%d %d", &c, &n ) == 2 ) {
		if ( offset + n < 0 ) offset = - n;
		C[ offset + n ] = c;
		tot += c;
	}

	for( i = 0; i <= n + offset; i++ ) printf( "%d %f\n", i - offset, C[i] / tot );

	return 0;
}
