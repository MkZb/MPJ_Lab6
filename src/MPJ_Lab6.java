import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.*;

public class MPJ_Lab6 {
    static int N = 400; //Matrix and array size.
    static int P = 4; //Threads count. Set it so N is multiple of P.

    //Only read data
    static float[][] MD;
    static float[][] MT;
    static float[][] MZ;
    static float[] B;
    static float[] D;

    //Write data (only thread with pNum = 0 writes them)
    static float[][] MA = new float[N][N];
    static float[] E = new float[N];

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        BlockingQueue<Message> sharedDataGather = new LinkedBlockingQueue<>();
        BlockingQueue<Message> sharedDataSend = new LinkedBlockingQueue<>();
        CountDownLatch waitEnd = new CountDownLatch(1);

        System.out.println("Program started");
        Data data = new Data(N);
        data.loadData("test2.txt");
        MD = data.parseMatrix(N);
        MT = data.parseMatrix(N);
        MZ = data.parseMatrix(N);
        B = data.parseVector(N);
        D = data.parseVector(N);
        System.out.println("Data successfully parsed");
        ExecutorService es = Executors.newFixedThreadPool(P);

        for (int i = 0; i < P; i++) {
            es.execute(new singleT(N, P, i, B, D, E, MD, MT, MZ, MA, waitEnd, sharedDataGather, sharedDataSend));
        }

        try {
            waitEnd.await();
            es.shutdown();
            long finish = System.currentTimeMillis();
            long timeExecuted = finish - start;
            File resultMA = new File("resultMA.txt");
            File resultE = new File("resultE.txt");
            FileWriter writer1 = new FileWriter("resultMA.txt");
            FileWriter writer2 = new FileWriter("resultE.txt");
            for (int j = 0; j < N; j++) {
                for (int k = 0; k < N; k++) {
                    //System.out.print(MA[j][k] + " ");
                    writer1.write(MA[j][k] + "\n");
                }
                //System.out.println();
            }

            for (int j = 0; j < N; j++) {
                //System.out.print(E[j] + " ");
                writer2.write(E[j] + "\n");
            }
            //System.out.println();
            writer1.close();
            writer2.close();
            System.out.println("Data successfully saved on disk");
            System.out.println(timeExecuted + " milliseconds spent on calculations");
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class singleT implements Runnable {
    private final BlockingQueue<Message> sharedDataGather;
    private final BlockingQueue<Message> sharedDataSend;
    private final CountDownLatch waitEnd;
    private final int N;
    private final int P;
    private final int pNum;

    private final float[][] MD;
    private final float[][] MT;
    private final float[][] MZ;
    private final float[][] MA;
    private final float[] B;
    private final float[] D;
    private final float[] E;
    private float a = 0;

    public singleT(int N, int P, int pNum, float[] B, float[] D, float[] E,
                   float[][] MD, float[][] MT, float[][] MZ, float[][] MA, CountDownLatch waitEnd,
                   BlockingQueue<Message> sharedDataGather, BlockingQueue<Message> sharedDataSend) {
        this.sharedDataGather = sharedDataGather;
        this.sharedDataSend = sharedDataSend;
        this.waitEnd = waitEnd;
        this.N = N;
        this.P = P;
        this.pNum = pNum;
        this.MD = MD;
        this.MT = MT;
        this.MZ = MZ;
        this.MA = MA;
        this.B = B;
        this.D = D;
        this.E = E;
    }

    public void run() {
        float maxMD = 0;
        float[][] MTZpart = new float[N][N / P];
        float[][] MTDpart = new float[N][N / P];

        System.out.println("Thread " + pNum + " started");
        //Calc max(MD)
        for (int j = 0; j < N; j++) {
            for (int k = (N / P) * pNum; k < (N / P) * (pNum + 1); k++) {
                if (MD[j][k] > maxMD) maxMD = MD[j][k];
            }
        }

        try {
            sharedDataGather.put(new Message(1, pNum, maxMD));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (pNum == 0) {
            for (int i = 0; i < P; i++) {
                try {
                    Message msg = sharedDataGather.take();
                    if (msg.getType() != 1) {
                        sharedDataGather.put(msg);
                        i--;
                        continue;
                    }
                    if (msg.getNumber() > a) a = msg.getNumber();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            for (int i = 0; i < P; i++) {
                try {
                    sharedDataSend.put(new Message(1, pNum, a));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //Calc B*MD+D*MT
        float[] E_part = new float[N / P];
        for (int j = (N / P) * pNum; j < (N / P) * (pNum + 1); j++) {
            float[] arrayToAdd = new float[2 * N];
            for (int k = 0; k < N; k++) {
                arrayToAdd[k] += B[k] * MD[j][k];
                arrayToAdd[k + N] += D[k] * MT[j][k];
            }
            Arrays.sort(arrayToAdd);
            float res = 0;
            for (int k = 0; k < 2 * N; k++) {
                res += arrayToAdd[k];
            }
            E_part[j - (N / P) * pNum] = res;
        }

        try {
            sharedDataGather.put(new Message(2, pNum, E_part));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            a = sharedDataSend.take().getNumber();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //Calc max(MD)*(MT+MZ)
        for (int j = 0; j < N; j++) {
            for (int k = (N / P) * pNum; k < (N / P) * (pNum + 1); k++) {
                MTZpart[j][k - (N / P) * pNum] = a * (MT[j][k] + MZ[j][k]);
            }
        }

        //Calc max(MD)*(MT+MZ)-MT*MD
        float[][] MA_part = new float[N][N / P];
        for (int j = 0; j < N; j++) {
            for (int k = (N / P) * (pNum); k < (N / P) * (pNum + 1); k++) {
                float[] arrayToAdd = new float[N];
                for (int l = 0; l < N; l++) {
                    arrayToAdd[l] = MT[j][l] * MD[l][k];
                }
                Arrays.sort(arrayToAdd);
                MTDpart[j][k - (N / P) * (pNum)] = 0;
                for (int l = 0; l < N; l++) {
                    MTDpart[j][k - (N / P) * (pNum)] += arrayToAdd[l];
                }
                MA_part[j][k - (N / P) * (pNum)] = MTZpart[j][k - (N / P) * (pNum)] - MTDpart[j][k - (N / P) * (pNum)];
            }
        }

        try {
            sharedDataGather.put(new Message(3, pNum, MA_part));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (pNum == 0) {
            for (int i = 0; i < 2 * P; i++) {
                try {
                    Message msg = sharedDataGather.take();
                    if (msg.getType() == 2) {
                        float[] arr = msg.getArr();
                        int p = msg.getpNum();
                        for (int j = (N / P) * p; j < (N / P) * (p + 1); j++) {
                            E[j] = arr[j - (N / P) * p];
                        }
                    } else if (msg.getType() == 3) {
                        float[][] matrix = msg.getMatrix();
                        int p = msg.getpNum();
                        for (int j = 0; j < N; j++) {
                            for (int k = (N / P) * p; k < (N / P) * (p + 1); k++) {
                                MA[j][k] = matrix[j][k - (N / P) * p];
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            waitEnd.countDown();
        }
    }
}

class Message {
    private final int type;
    private final int pNum;
    private float number;
    private float[] arr;
    private float[][] matrix;

    public Message(int type, int pNum, float num) {
        this.type = type;
        this.pNum = pNum;
        this.number = num;
    }

    public Message(int type, int pNum, float[] arr) {
        this.type = type;
        this.pNum = pNum;
        this.arr = arr;
    }

    public Message(int type, int pNum, float[][] matrix) {
        this.type = type;
        this.pNum = pNum;
        this.matrix = matrix;
    }

    public int getType() {
        return type;
    }

    public int getpNum() {
        return pNum;
    }

    public float getNumber() {
        if (type == 1) {
            return number;
        } else {
            throw new RuntimeException();
        }
    }

    public float[] getArr() {
        if (type == 2) {
            return arr;
        } else {
            throw new RuntimeException();
        }
    }

    public float[][] getMatrix() {
        if (type == 3) {
            return matrix;
        } else {
            throw new RuntimeException();
        }
    }
}