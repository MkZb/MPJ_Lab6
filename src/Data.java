import java.io.FileNotFoundException;
import java.util.Random;
import java.util.Scanner;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Data {
    int size;
    float[] data;
    int pointerPos = 0;

    public Data(int N) {
        size = (N * N) * 3 + N * 2;
        data = new float[size];
    }

    public boolean isFileExist(String fileName) {
        return new File(fileName).isFile();
    }

    private void genNewData() {
        Random randomiser = new Random();

        for (int i = 0; i < size; i++) {
            data[i] = randomiser.nextFloat(); //Random float numbers
            //data[i] = 1; //Data filled with 1
        }
    }

    /**
     * Loads data from the file if it exists.
     * Create new data and save it otherwise.
     *
     * @param fileName File name of the file attempting to load from.
     */
    public void loadData(String fileName) {
        if (isFileExist(fileName)) {
            try {
                File file = new File(fileName);
                Scanner scanner = new Scanner(file);
                int loadedSize = Integer.parseInt(scanner.nextLine());
                if (loadedSize != size) {
                    throw new Exception();
                }
                for (int i = 0; i < size; i++) {
                    data[i] = Float.parseFloat(scanner.nextLine());
                }
                scanner.close();
                System.out.println("Data successfully loaded");
            } catch (FileNotFoundException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            } catch (Exception e) {
                System.out.println("An error occurred size of loaded data does not match. Consider changing N or create new data file.");
                e.printStackTrace();
            }
        } else {
            genNewData();
            try {
                File file = new File(fileName);
                FileWriter writer = new FileWriter(fileName);
                writer.write(String.valueOf(size));
                for (int i = 0; i < size; i++) {
                    writer.write("\n" + data[i]);
                }
                writer.close();
                System.out.println("Data successfully created and saved");
            } catch (IOException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }
        }
    }

    /**
     * Parses matrix of defined size from previously loaded data.
     *
     * @param N Size of matrix to fill.
     * @return New matrix object with parsed data inside.
     */
    public float[][] parseMatrix(int N) {
        float[][] matrix = new float[N][N];
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < N; j++) {
                matrix[i][j] = data[pointerPos];
                pointerPos++;
            }
        }
        return matrix;
    }

    /**
     * Parses vector of defined size from previously loaded data.
     *
     * @param N Size of vector to fill.
     * @return New vector object with parsed data inside.
     */
    public float[] parseVector(int N) {
        float[] vector = new float[N];
        for (int i = 0; i < N; i++) {
            vector[i] = data[pointerPos];
            pointerPos++;
        }
        return vector;
    }
}
