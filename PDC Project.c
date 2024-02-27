#include <stdio.h>

#include <mpi.h>



#define MAT_SIZE 2



void mapper(int keyValPair[]) {

    // Multiply the values of i and j index (last 2 elements of the array) and print the result. Return the result.

    int result = keyValPair[2] * keyValPair[3];

    printf("Mapper: (%d, %d) -> %d\n", keyValPair[0], keyValPair[1], result);

    int mapperPair[MAT_SIZE*MAT_SIZE];

    mapperPair[0] = keyValPair[0];

    mapperPair[1] = keyValPair[1];

    mapperPair[2] = result;

    int n = 0;

    MPI_Send(&mapperPair, 3, MPI_INT, n , 0, MPI_COMM_WORLD);

}



void shuffler(){

     int index = 0;

     int shufflerArray[MAT_SIZE*MAT_SIZE*MAT_SIZE][MAT_SIZE*MAT_SIZE];

     int toReducer[MAT_SIZE*MAT_SIZE*MAT_SIZE][MAT_SIZE*MAT_SIZE]; 	

     int shufflerPair[MAT_SIZE*MAT_SIZE];

     	for(int i = 0; i<MAT_SIZE*MAT_SIZE*MAT_SIZE;i++){

		MPI_Recv(&shufflerPair, 3, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

	        printf("In shuffler: (%d, %d) -> %d\n", shufflerPair[0], shufflerPair[1], shufflerPair[2]);

	

		shufflerArray[i][0] = shufflerPair[0];

		shufflerArray[i][1] = shufflerPair[1];

		shufflerArray[i][2] = shufflerPair[2];

	 

	}



	for(int i = 0; i<MAT_SIZE*MAT_SIZE*MAT_SIZE; i++){

		printf("2D ARRAY OUTPUT: %d, %d, %d\n", shufflerArray[i][0], shufflerArray[i][1], shufflerArray[i][2]);

	

	

}





int i, j, k, flag, temp;

    int combinedArray[MAT_SIZE*MAT_SIZE*MAT_SIZE][MAT_SIZE*MAT_SIZE];

    int combinedCount = 0;



    for(i = 0; i < MAT_SIZE*MAT_SIZE*MAT_SIZE; i++) {

        flag = 0;

	int ind = 3;

	if(shufflerArray[i][0] == -1){	

//	printf("YES");

	continue;

	}

	

	combinedArray[combinedCount][0] = shufflerArray[i][0];

        combinedArray[combinedCount][1] = shufflerArray[i][1];

	combinedArray[combinedCount][2]	= shufflerArray[i][2];

        for(j = i+1; j < MAT_SIZE*MAT_SIZE*MAT_SIZE; j++) {

            if(shufflerArray[i][0] == shufflerArray[j][0] && shufflerArray[i][1] == shufflerArray[j][1]) {

                flag = 1;

    //            break;

	}



	if(flag) {

		

            combinedArray[combinedCount][ind] = shufflerArray[j][2];

	    ind++;



            shufflerArray[j][0] = -1; // mark the other element as used

            shufflerArray[j][1] = -1;

            shufflerArray[j][2] = 0;

		flag = 0;

        }

	

} 



combinedCount++;

}



    int n = 1;

    // output the new combined array

    for(i = 0; i <MAT_SIZE*MAT_SIZE; i++) {

        printf("%d %d %d %d\n", combinedArray[i][0], combinedArray[i][1], combinedArray[i][2], combinedArray[i][3]);

	 MPI_Send(&combinedArray[i], MAT_SIZE + 2, MPI_INT, n , 0, MPI_COMM_WORLD);

         n++;

    }



}



void reducer(){

	int sum = 0;	

	int reducerPair[MAT_SIZE*MAT_SIZE];

	MPI_Recv(&reducerPair, MAT_SIZE+2, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

	printf("In reducer: %d, %d, %d, %d\n", reducerPair[0], reducerPair[1], reducerPair[2], reducerPair[3]);

	for(int i = 2; i<MAT_SIZE+2;i++){

		sum = sum+reducerPair[i];	

	}

	//printf("%d", sum);

//open file in append mode

   FILE *fp;



   fp = fopen("ReducerMatrix4.txt", "a");



   if(fp == NULL) {

      printf("Error opening file.\n");

   }	

   fprintf(fp, "%d, %d, %d\n", reducerPair[0], reducerPair[1], sum );





   fclose(fp);



	



	





}





int main(int argc, char** argv) {

    int output[MAT_SIZE][MAT_SIZE];

    int rank, size;

    int index = 0;

    int num = 0;

    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    MPI_Comm_size(MPI_COMM_WORLD, &size);



if(rank==0){

  int A[MAT_SIZE][MAT_SIZE] = {{1, 2}, {3, 4}};

    int B[MAT_SIZE][MAT_SIZE] = {{5, 6}, {7, 8}};



    int keyValPairs[MAT_SIZE * MAT_SIZE ];

    int n = 1;



    if (size < MAT_SIZE * MAT_SIZE) {

        fprintf(stderr, "Error: number of processes must be equal to matrix size squared\n");

        MPI_Abort(MPI_COMM_WORLD, 1);

   } 



    for (int i = 0; i < MAT_SIZE; i++) {

        for (int j = 0; j < MAT_SIZE; j++) {

            for (int k = 0; k < MAT_SIZE; k++) {



               keyValPairs[0] = i;

               keyValPairs[1] = j;

               keyValPairs[2] = A[i][k];

               keyValPairs[3] = B[k][j];

//	       printf("%d %d %d %d", i , j, A[i][k], B[k][j]);

              //send  Mpisend

      	      MPI_Send(&keyValPairs, 4, MPI_INT, n , 0, MPI_COMM_WORLD);

		n++;

		printf("Sendding: %d, %d, %d, %d" , keyValPairs[0], keyValPairs[1], keyValPairs[2], keyValPairs[3]);

            }

        }

   }



}





else{

	int recvkeyvalpairs[MAT_SIZE * MAT_SIZE];

	MPI_Recv(&recvkeyvalpairs, 4, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

	printf("recieveing: rank: %d, (%d, %d), %d, %d\n", rank, recvkeyvalpairs[0],recvkeyvalpairs[1], recvkeyvalpairs[2], recvkeyvalpairs[3]);

	mapper(recvkeyvalpairs);

}



if(rank==0){

      shuffler();

}



else if(rank<=4){

      reducer();

	



}



if(rank==0){

	//file reading AND put values in resultant matrix

   FILE *fp;

   int data[MAT_SIZE+2][MAT_SIZE+2];

   int num, row, col;



   fp = fopen("ReducerMatrix2.txt", "r");



   if(fp == NULL) {

      printf("Error opening file.\n");

      return 1;

   }



   for(row = 0; row < MAT_SIZE+2; row++) {

      for(col = 0; col < MAT_SIZE+1; col++) {

         if(fscanf(fp, "%d", &num) != 1) {

            printf("Error reading file.\n");

            return 1;

         }

         data[row][col] = num;

         // Skip the comma

         fgetc(fp);

      }

   }



   printf("The data in the file is:\n");

   for(row = 0; row < MAT_SIZE+2; row++) {

      for(col = 0; col < MAT_SIZE+1; col++) {

         printf("%d ", data[row][col]);

      }

      printf("\n");

   }



   fclose(fp);

  for(row = 0; row < MAT_SIZE; row++) {

      for(col = 0; col < MAT_SIZE; col++) {

                 int rows = data[row][0];

                 int cols = data[row][1];

                 int vals = data[row][2];

                output[rows][cols] = vals;

            

        }

    }

	printf("Final Array: \n");

	printf("%d,%d\n,%d,%d\n", data[2][2], data[1][2], data[3][2], data[0][2]);	

//for (int i = 0; i < MAT_SIZE; i++) {

  //      for (int j = 0; j < MAT_SIZE; j++) {

    //            printf("FINAL: %d\n ", output[i][j]);

            

      //  }

   // }



}

	MPI_Finalize();



	return 0;

}

