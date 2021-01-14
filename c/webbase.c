#include <stdio.h>
#include <string.h>
#include <ctype.h>

unsigned char b[ 10000000 ], u[ 10000000 ];

void fixCR( char *s ) {
	int l = strlen( s );
	if ( l == 0 ) return;
	if ( s[ l - 1 ] == '\r' ) s[ l - 1 ] = 0;
}

void fixProtocol( unsigned char *s ) {
	while( isalpha( *s ) ) {
		*s = tolower( *s );
		s++;
	}
}

int main(void) {
	int i, p = -1;
	int skip = 0;
	long long seen = 0, output = 0;

	for(;;) {

		if ( gets( b ) == NULL ) {
			printf( "\n" );
			fprintf( stderr, "Pages: %d Links seen: %lld Links output: %lld\n", p, seen, output );
			return 0;
		}

		if ( strcmp( b, "==P=>>>>=i===<<<<=T===>=A===<=!Junghoo!==>" ) == 0 ) { /* We found the magic cookie */
			gets( b ); /* This line *MUST* be a URL. */
			fixCR( b );
			if ( strncmp( b, "URL: ", 5 ) != 0 ) { /* If it is not, we just stop. */
				fprintf( stderr, "Stopping at page %d--no URL found (found %s instead).\n", p, b );
				return 1;
			}
			strcpy( u, b ); /* We keep the URL in u for later reference. */

			/* We not want to put robots.txt in our database. */
			skip = ( (unsigned char *)strstr( u, "robots.txt" ) - u ) == ( strlen( u ) - strlen( "robots.txt" ) );

			i = strlen( u );
			while( i-- != 5 ) if ( u[ i ] <= 32 ) {
				fprintf( stderr, "Control or space character (%d) in URL %s at page %d--skipping this entry.\n", u[ i ], u, p );
				skip = 1;
			}

			if ( strncasecmp( "http", b + 5, 4 ) ) {
				fprintf( stderr, "URL %s at page %d does not start with \"http\"--skipping this entry.\n", u, p );
				skip = 1;
			}

			if ( ! skip ) {
				if ( p % 1000000 == 0 ) fprintf( stderr, "Pages: %d Links seen: %lld Links output: %lld\n", p, seen, output );
				if ( p != - 1 ) printf( "\n" ); /* We terminate the previous URL list. */
				p++; /* Next page */
				fixProtocol( b + 5 );
				printf( "%s", b + 5 ); /* Print the page URL */
			}

			gets( b ); /* We skip three lines... */
			gets( b );
			gets( b ); 
			if ( b[ 0 ] != 0 ) /* ...and we check that the third one is empty. */
				fprintf( stderr, "Warning at page %d, URL %s: out of sync on the third skipped line.\n", p, u );
		}
		else { /* We are scanning a successor list. */
			if ( ! skip ) {
				seen++;

				i = strlen( b );
				while( i-- != 0 ) if ( b[ i ] <= 32 ) break;
				if ( i >= 0 ) continue;

				if ( ! strncmp( "http", b, 4 ) ) {
					output++;
					fixCR( b );
					fixProtocol( b );
					printf("\t%s", b );
				}
			}
		}      
	}
}
