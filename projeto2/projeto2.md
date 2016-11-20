Projeto de Implementação 2
===========================

Pairs Implementation
---------------------
Command: hadoop jar target/projeto2-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.anibrata.PairsPMI -input <Shakespeare.txt/simplewiki.txt> -output npairs -reducers 1 -imc <y/n> 

-imc - y for enabling in-mapper-combining and n to deactivate

Mapper1 - Input  : Shakespeare.txt file
	  Output : (Term, 1) for each of the distinct terms in the collection.

In Mapper Combiner - Input : (Term, 1) for each of the distinct terms in the collection
	  Output : (Term, Frequency) for each distinct term of the collection

Reducer1 - Input : (Term, Frequency (in case of In Mapper Combining)/ 1(without combiner)) for each term
	  Output : (Term, Final Frequency) for all terms in the collection

Mapper2 - Input : Shakespeare.txt file
	 Output : [(Term1, Term2), 1] for all terms of the collection where term1 and term2 are different. Use a map / associative array for ease.

Combiner - Input : Output of Mapper2
	  Output : Generate [(Term1, Term2), Co-occurences] for each pair of distinct terms in the collection

Reducer - Input : Output of Combiner above & Output of Reducer1(The data is stored in the memory in a map to access termwise frequency and total number of terms)
	 Output : Calculates the PMI from the co-occurences of the terms and generates the final output as [(term1, term2), PMI(1,2)]


Stripes Implementation
-----------------------
Command: hadoop jar target/projeto2-1.0-SNAPSHOT-fatjar.jar br.edu.ufam.anibrata.StripesPMI -input <Shakespeare.txt/simplewiki.txt> -output nstripes -reducers 1 -imc <y/n> 

-imc - y for enabling in-mapper-combining and n to deactivate

Mapper1 - Input  : Shakespeare.txt file
	  Output : (Term, 1) for each of the distinct terms in the collection.

In Mapper Combiner - Input : (Term, 1) for each of the distinct terms in the collection
	  Output : (Term, Frequency) for each distinct term of the collection

Reducer1 - Input : (Term, Frequency (in case of In Mapper Combining)/ 1(without combiner)) for each term
	  Output : (Term, Final Frequency) for all terms in the collection

Mapper2 - Input : Shakespeare.txt file
	 Output : [Term1, (Term2, 1), (Term3, 1), ...] for all terms of the collection where term1 and term2 are different. Use a hashmap / treeset.

Combiner - Input : Output of Mapper2
	  Output : Generate [Term1, (Term2, Co-occurences), (Term3, Co-occurences), ...] for each pair of distinct terms in the collection.

Reducer - Input : Output of Combiner above & Output of Reducer1(The data is stored in the memory in a map to access termwise frequency and total number of terms)
	 Output : Uses HashMap again to calculate the PMI from the co-occurences of the terms and generates the final output as [(term1, term2), PMI(1,2)]

Note: Run script results.sh to get the results of question 3-5 and 8-10.


Data for Shakespeare's Combined works text file 
------------------------------------------------
1. Average execution time for Pairs: 82.22 seconds. Average execution time for Stripes: 115.83 seconds.

2. Average execution time for Pairs without combiners: 80.81 seconds. Average execution time for Stripes without combiners: 112.32 seconds. 

3. Number of distinct pairs :41007


4. Pair with highest PMI - (milford, haven). 
For this pair the probability of occurence of the pair of the words is comparatively less than the probability of the occurence of the terms individually. Thus these words do not occur very frequently in the collection, and hence there co-occurence carries importance and thus, information. Most probably in this case the words mean together to be the name of some place.


5. Terms with highest PMI with the word life and the PMI values

	a. (save, life)	  2.098221472198712
	b. (man's, life)  1.8259068318699443
	c. (dear, life)   1.4156490808506554


  Terms with highest PMI with the word love and the PMI values

	a. (thomas, lovell)   3.5100013999006685
	b. (she, loved)       2.008814288516852
	c. (sir, lovell)      1.9037060422132384


Data for Simple Wiki text file
-------------------------------
6. Average execution time for Pairs: 71.78 seconds. Average execution time for Stripes: 109.49 seconds.

7. Average execution time for Pairs without combiners: 68.86 seconds. Average execution time for Stripes without combiners: 105.57 seconds. 

8. Number of distinct pairs :4035


9. Pair with highest PMI - (elías, piña)


10. Terms with highest PMI with the word life and the PMI values

	a. (a, life)	1.5435312993899812
	b. (life, and)	1.2205642048287124
	c. (the, life)	1.104461451493375


   Terms with highest PMI with the word love and the PMI values

	a. (my, love)	2.2471659327384965
	b. (love, song)	1.7933617421466947
	c. (i, love)	1.7846852042017243

