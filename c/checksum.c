/* Computes a checksum of all non-control characters in the file. The checksum is resilient to permutations. */

#include <stdio.h>

int main(void) {
	int c;
	long long check = 0;

	while( ( c = getchar() ) != -1 ) if ( c > 32 ) check += c;

	printf( "%lld\n", check );

	return 0;
}
