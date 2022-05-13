import java.net.*;
import java.util.Scanner;
import java.io.*;

public class test {
    public static void main(String[] args) {
        try {
            System.out.println("Please enter ip and port: ");
            Scanner in = new Scanner(System.in);
            String addr = in.nextLine();
            String[] ipAndPort = addr.split(":");
            Socket socket = new Socket(ipAndPort[0], Integer.parseInt(ipAndPort[1]));
            DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
            while (true) {
                // System.out.print("Please enter opcode: ");
                // int opcode = in.nextInt();
                // dos.writeInt(opcode);
                System.out.print("Please enter number of terms: ");
                int num = in.nextInt();
                in.nextLine();
                for (int i = 0; i < num; i++) {
                    System.out.print("Please enter term " + i + ": ");
                    String term = in.nextLine();
                    dos.writeUTF(term);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
