import java.io.Serializable;
import java.util.Arrays;
import java.util.Scanner;

import mpi.MPI;
public class BitonicSortMPI 
{
	public static void main(String[] args)
	{
		MPI.Init(args);
	    int rank = MPI.COMM_WORLD.Rank();
	    int N = 64;
	    
	    Object arr[] = new Object[N];
	    
	    if(rank == 0)
	    {
	    	for(int i = 0 ; i < N; i++)
	    	{
	    		arr[i] = (int)(Math.random()*1000);
	    	}
	    	Arrays.sort(arr);
	    	Object dataArr[] = new Object[arr.length];
	    	int dataArrIndex = 0;
	    	for(int i = 0 ; i < arr.length / 2; i++)
	    	{
	    		dataArr[dataArrIndex++] = arr[i];
	    	}
	    	for(int i = arr.length - 1; i>= arr.length / 2; i--)
	    	{
	    		dataArr[dataArrIndex++] = arr[i];
	    	}
//	    	Object dataArr[] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,16,15,14,13,12,11,10,9,8,7,6,5,4,3,2,1};
	    	arr = dataArr;
	    }
	    int p = MPI.COMM_WORLD.Size();
//	    System.out.println(p);
	    int times = 0;
//	    if(rank == 0)
	    	times = (int)(Math.round(Math.log(arr.length)/ Math.log(2)));
	    int processorDataSize = arr.length/(2*p);
	   
	    for(int i = 0 ; i < times; i++)
	    {
	    	int pow = (int)Math.pow(2, i+1);
	    	int step = arr.length/ pow;
	    	
	    	DataPoint data1[] = new DataPoint[arr.length/2];
	    	DataPoint data2[] = new DataPoint[arr.length/2];
	    	int itr = 0;
	    	int index = 0;
	    	int index2 = 0;
	    	int start = 0;
	    	while(itr < Math.pow(2, i))
	    	{
	    		for(int j = start; j < start + step; j++)
	    		{
	    			if(rank == 0)
	    			{
	    				data1[index++] = new DataPoint((int)arr[j], j);
	    				data2[index2++] = new DataPoint((int)arr[(int)(j + Math.pow(2, times-i-1))],(int)(j + Math.pow(2, times-i-1))) ;
	    			}
	    		}
	    		start = start + (int)Math.pow(2, times-i);
	    		itr++;
	    	}
	    	
	    	int tag = 0;
	    	for(int j = 0 ; j < p-1; j++)
	    	{
	    		if(rank == 0)
	    		{
	    			MPI.COMM_WORLD.Send(data1, j*processorDataSize, processorDataSize, MPI.OBJECT, j+1, tag);
		    		MPI.COMM_WORLD.Send(data2, j*processorDataSize, processorDataSize, MPI.OBJECT, j+1, tag+1);
		    		System.out.println("sent to -" + (j+1) );
	    		}
	    		
	    	}
	    	if(rank != 0)
		    {
//	    		System.out.println("Hello");
		    	
//		    	System.out.println("1");
		    	DataPoint d1[] = new DataPoint[processorDataSize];
		    	DataPoint d2[] = new DataPoint[processorDataSize];
//			    		 public mpi.Status Recv(java.lang.Object buf, int offset, int count, mpi.Datatype datatype, int source, int tag) throws mpi.MPIException;
			    MPI.COMM_WORLD.Recv(d1, 0, processorDataSize, MPI.OBJECT, 0, tag);
			    MPI.COMM_WORLD.Recv(d2, 0, processorDataSize, MPI.OBJECT, 0, tag + 1);
//			    System.out.println("2");
			    System.out.println("received by -" + (rank));
//			    System.out.println(Arrays.toString(d1));
			    for(int k = 0 ; k < d1.length; k++)
			    {
			    	if(d1[k].data > d2[k].data)
			    	{
			    		int tempD = d1[k].data;
			    		d1[k].data = d2[k].data;
			    		d2[k].data = tempD;
			    	}	
			    }
//			    System.out.println("Hello");
			    MPI.COMM_WORLD.Send(d1, 0, processorDataSize, MPI.OBJECT, 0, tag);
			    MPI.COMM_WORLD.Send(d2, 0, processorDataSize, MPI.OBJECT, 0, tag + 1);
		    }
	    	
	    	if(rank == 0)
	    	{
	    		for(int k = 0 ; k < processorDataSize; k++)
			    {
	    			int indexK = (p-1)*processorDataSize + k;
			    	if(data1[indexK].data > data2[indexK].data)
			    	{
			    		int tempD = data1[indexK].data;
			    		data1[indexK].data = data2[indexK].data;
			    		data2[indexK].data = tempD;
			    	}	
			    	arr[data1[indexK].index] = data1[indexK].data;
	    			arr[data2[indexK].index] = data2[indexK].data;
			    }
	    		
	    		for(int j = 0; j < p-1; j ++)
	    		{
	    			DataPoint d1[] = new DataPoint[processorDataSize];
	    			DataPoint d2[] = new DataPoint[processorDataSize];
	    			MPI.COMM_WORLD.Recv(d1, 0, processorDataSize, MPI.OBJECT, j+1, tag);
		    		MPI.COMM_WORLD.Recv(d2, 0, processorDataSize, MPI.OBJECT, j+1, tag + 1);
		    		
		    		for(int k = 0 ; k < d1.length; k++)
		    		{
		    			arr[d1[k].index] = d1[k].data;
		    			arr[d2[k].index] = d2[k].data;
		    		}
	    		}
	    	}
	    	MPI.COMM_WORLD.Barrier();
	    
	    	System.out.println();
	    }
	    if(rank == 0)
	    	System.out.println(Arrays.toString(arr));
	    MPI.Finalize();
	}
	public static void createBitonicSequenceArray()
	{
		
	}
}

class DataPoint implements Serializable
{
	int data;
	int index;
	
	DataPoint(int data, int index)
	{
		this.data = data;
		this.index = index;
	}
	
	public String toString()
	{
		String ans = "";
		ans = ans + data + "--> " + index + "\n";
		return ans;
	}
}
