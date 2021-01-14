/*
        Illustrative example of minimum-redundancy code calculation.

        Program reads a file of symbol frequencies
            -- one per line
            -- ascending frequency
            -- first line indicates upper bound on how many 
                    frequencies appear;
        calculate the codeword lengths for a minimum-redundancy code;
        print out the codeword lengths;
        print out the average cost per symbol and the entropy.

        The method for calculating codelengths in function
        calculate_minimum_redundancy is described in 

        @inproceedings{mk95:wads,
                author = "A. Moffat and J. Katajainen",
                title = "In-place calculation of minimum-redundancy codes",
                booktitle = "Proc. Workshop on Algorithms and Data Structures",
                address = "Queen's University, Kingston, Ontario",
                publisher = "LNCS 955, Springer-Verlag",
                Month = aug,
                year = 1995,
                editor = "S.G. Akl and F. Dehne and J.-R. Sack",
                pages = "393-402",
        }

        The abstract of that paper may be fetched from 
        http://www.cs.mu.oz.au/~alistair/abstracts/wads95.html
        A revised version is currently being prepared.

        Written by
		Alistair Moffat, alistair@cs.mu.oz.au,
		Jyrki Katajainen, jyrki@diku.dk
	November 1996.
*/

#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#define log2(x) (log10((double)(x))/log10(2.0))


void
calculate_minimum_redundancy(long long *, long long);

#define LIMIT 1000000000                        /* maximum value for n */

int
main(int argc, char **argv) {

        long long *A;                              /* the array of freqs */
        long long *B;                              /* a copy of A */
        long long n;                               /* number of symbols */
        long long totfreq;                         /* total frequency */
        long long bits;                            /* total bits in codewords */
        double ent;                          /* entropy */

        long long i;

        /* claim space for arrays */
        scanf("%lld", &n);
        if (n <= 0 || n > LIMIT) {
                fprintf(stderr, "%s: n should be at least 0 and less than %d\n",
                        argv[0], LIMIT);
                exit(1);
        }
        if ((A = (long long *)malloc(n*sizeof(long long))) == (long long *)NULL
            || (B = (long long *)malloc(n*sizeof(long long))) == (long long *)NULL) {
                    fprintf(stderr, "%s: unable to allocate memory\n",
                        argv[0]);
                exit(1);
        }

        /* read symbol frequencies */
        for (i=0; i<n; i++) {
                if (scanf("%lld", A+i) == EOF) {
                        n = i;
                        break;
                }
        }

        /* calculate entropy, and check for sensible input values */
        totfreq = 0;
        for (i=0; i<n; i++) {
                totfreq += A[i];
                if (A[i] < 0 || (i>0 && A[i-1] > A[i])) {
                        fprintf(stderr, "%s: %s %s\n",
                                argv[0],
                                "input frequencies must be non-negative",
                                "and non-decreasing");
                        exit(1);
                }
        }
        if (totfreq <= 0) {
                fprintf(stderr, "%s: sum of frequencies must be positive\n",
                        argv[0]);
                exit(1);
        }
        ent = 0.0;
        for (i=0; i<n; i++) {
                double prob = (double)A[i]/totfreq;
                ent += - prob * log2(prob);
        }


        /* make a copy of A into B so that average weighted codeword
           length can be calculated once the codeword lengths are known;
           note that B is _not_ used by the length calculation process */
        for (i=0; i<n; i++) {
                B[i] = A[i];
        }

        /* now calculate the minimum-redundancy codeword lengths */
        calculate_minimum_redundancy(A, n);

        /* and evaluate average codeword length */
        bits = 0;
        for (i=0; i<n; i++) {
                bits += A[i] * B[i];
        }

        /*  and print the results */
	if (n <= 100) {
		for (i=0; i<n; i++) {
			printf("f_%02lld = %4lld, |c_%02lld| = %2lld\n",
				i, B[i], i, A[i]);
		}
	}
        printf("entropy                 = %5.2f bits per symbol\n", ent);
        printf("minimum-redundancy code = %5.2f bits per symbol\n", 
                (double)bits/totfreq);
	if (ent > 0.0)
		printf("inefficiency            = %5.2f%%\n", 
			100*(double)bits/totfreq/ent - 100.0);
        
	free(A);
	free(B);
	return(0);
}



/*** Function to calculate in-place a minimum-redundancy code
     Parameters:
        A        array of n symbol frequencies, non-decreasing
        n        number of symbols in A
*/
void
calculate_minimum_redundancy(long long A[], long long n) {
        long long root;                  /* next root node to be used */
        long long leaf;                  /* next leaf to be used */
        long long next;                  /* next value to be assigned */
        long long avbl;                  /* number of available nodes */
        long long used;                  /* number of internal nodes */
        long long dpth;                  /* current depth of leaves */

        /* check for pathological cases */
        if (n==0) { return; }
        if (n==1) { A[0] = 0; return; }

        /* first pass, left to right, setting parent pointers */
        A[0] += A[1]; root = 0; leaf = 2;
        for (next=1; next < n-1; next++) {
                /* select first item for a pairing */
                if (leaf>=n || A[root]<A[leaf]) {
                        A[next] = A[root]; A[root++] = next;
                } else
                        A[next] = A[leaf++];

                /* add on the second item */
                if (leaf>=n || (root<next && A[root]<A[leaf])) {
                        A[next] += A[root]; A[root++] = next;
                } else
                        A[next] += A[leaf++];
        }
        
        /* second pass, right to left, setting internal depths */
        A[n-2] = 0;
        for (next=n-3; next>=0; next--)
                A[next] = A[A[next]]+1;

        /* third pass, right to left, setting leaf depths */
        avbl = 1; used = dpth = 0; root = n-2; next = n-1;
        while (avbl>0) {
                while (root>=0 && A[root]==dpth) {
                        used++; root--;
                }
                while (avbl>used) {
                        A[next--] = dpth; avbl--;
                }
                avbl = 2*used; dpth++; used = 0;
        }
}
