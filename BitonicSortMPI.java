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
	    int p = MPI.COMM_WORLD.Size();
	    
	    Object arr[] = new Object[N];
	    
	    if(rank == 0)
	    {
	    	arr = createBitonicSequenceArray(N);
	    	printBitonicSequence(arr);
	    }
	    
	    int times = (int)(Math.round(Math.log(arr.length)/ Math.log(2)));
	    int processorDataSize = arr.length/(2 * p);
	    DataPoint d1[] = new DataPoint[processorDataSize];
	    DataPoint d2[] = new DataPoint[processorDataSize];
	    
	    for(int i = 0 ; i < times; i++)
	    {	    	
	    	DataPoint data1[] = new DataPoint[arr.length / 2]; // sequences of ith data
	    	DataPoint data2[] = new DataPoint[arr.length / 2]; // sequences of i + 2^nth data

	    	populateDataForProcessors(data1, data2, i, times, arr);
	    	
	    	int tag = 0;
	    	for(int j = 0 ; j < p - 1; j++)
	    	{
	    		if(rank == 0)
	    		{
	    			MPI.COMM_WORLD.Send(data1, j * processorDataSize, processorDataSize, MPI.OBJECT, j + 1, tag);
		    		MPI.COMM_WORLD.Send(data2, j * processorDataSize, processorDataSize, MPI.OBJECT, j + 1, tag+1);
	    		}
	    		
	    	}
	    	if(rank != 0)
		    {

			    MPI.COMM_WORLD.Recv(d1, 0, processorDataSize, MPI.OBJECT, 0, tag);
			    MPI.COMM_WORLD.Recv(d2, 0, processorDataSize, MPI.OBJECT, 0, tag + 1);

			    swap(d1, d2, 0, d1.length);
			    
			    MPI.COMM_WORLD.Send(d1, 0, processorDataSize, MPI.OBJECT, 0, tag);
			    MPI.COMM_WORLD.Send(d2, 0, processorDataSize, MPI.OBJECT, 0, tag + 1);
		    }
	    	
	    	if(rank == 0)
	    	{
	    		swap(data1, data2, (p - 1) * processorDataSize, p * processorDataSize);

	    		updateOriginalArray(arr, data1, data2);
	    		
	    		for(int j = 0; j < p - 1; j ++)
	    		{
	    			MPI.COMM_WORLD.Recv(d1, 0, processorDataSize, MPI.OBJECT, j + 1, tag);
		    		MPI.COMM_WORLD.Recv(d2, 0, processorDataSize, MPI.OBJECT, j + 1, tag + 1);
		    		
		    		updateOriginalArray(arr, d1, d2);
	    		}
	    	}
	    	System.out.println();
	    }
	    if(rank == 0)
	    {
	    	System.out.println("The final Sorted Array is = ");
	    	System.out.println(Arrays.toString(arr));
	    }
	    	
	    MPI.Finalize();
	}
	
	public static void printBitonicSequence(Object arr[])
	{
		System.out.println("The Bitonic Sequence is ");
		System.out.println(Arrays.toString(arr));
	}
	
	public static Object[] createBitonicSequenceArray(int N)
	{
		Object o1[] = new Object[N / 2];
		Object o2[] = new Object[N / 2];
		
		for(int i = 0 ; i < N/2; i++)
    	{
    		o1[i] = (int)(Math.random()*1000);
    	}
    	Arrays.sort(o1);
    	
    	for(int i = 0 ; i < N/2; i++)
    	{
    		o2[i] = (int)(Math.random()*1000);
    	}
    	Arrays.sort(o2);
    	Object dataArr[] = new Object[N];
    	int dataArrIndex = 0;
    	for(int i = 0 ; i < N / 2; i++)
    	{
    		dataArr[dataArrIndex++] = o1[i];
    	}
    	for(int i = N/2 - 1; i >= 0; i--)
    	{
    		dataArr[dataArrIndex++] = o2[i];
    	}
   
    	return dataArr;
	}
	
	public static void populateDataForProcessors(DataPoint data1[], DataPoint data2[], int i, int times, Object arr[])
	{
		int itr = 0;
		int pow = (int)Math.pow(2, i+1);
		int step = arr.length/ pow;
    	int index = 0;
    	int index2 = 0;
    	int start = 0;
    	int rank = MPI.COMM_WORLD.Rank();
    	
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
	}
	
	public static void swap(DataPoint d1[], DataPoint d2[], int startIndex, int endIndex)
	{
	    for(int k = startIndex ; k < endIndex; k++)
	    {
	    	if(d1[k].data > d2[k].data)
	    	{
	    		int tempD = d1[k].data;
	    		d1[k].data = d2[k].data;
	    		d2[k].data = tempD;
	    	}	
	    }
	}
	
	public static void updateOriginalArray(Object arr[], DataPoint d1[], DataPoint d2[])
	{
		for(int k = 0 ; k < d1.length; k++)
		{
			arr[d1[k].index] = d1[k].data;
			arr[d2[k].index] = d2[k].data;
		}
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
