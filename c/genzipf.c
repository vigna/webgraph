#include <stdio.h>
#include <stdlib.h>
#include <math.h>

/* Generates a Zipf-like distribution for inplace.c. First emits the number of counts,
then nondecreasing counts that scale to a Zipf distribution of given exponent. */

int main( int argc, char **argv ) {
	long long i;
	double scale;

	if ( argc != 3 ) {
		fprintf( stderr, "Usage: %s <counts> <exponent>\n", argv[ 0 ] );
		return 0;
	}

	long long N = strtoll( argv[ 1 ], NULL, 0 );
	float expon = atof( argv[ 2 ] );

	printf( "%lld\n", N );

	scale = exp( -expon * log( N ) );

	for ( i = N; i >= 1; i-- ) {
		printf( "%lld\n", (long long)( exp( -expon * log( i ) ) / scale ) );
	}

	return 0;
}
