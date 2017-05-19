package neu.sw.hxs.ui;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class ListenerThread extends Thread {
    private MainFrame mainFrame;
    private ServerSocket serverSocket;

    ListenerThread(MainFrame mainFrame, int port) throws IOException {
        this.mainFrame = mainFrame;
        this.serverSocket = new ServerSocket(port);
    }

    @Override
    public void run() {
        while (true) {
            Socket accept;
            try {
                accept = serverSocket.accept();
                System.out.println(accept.getInetAddress() + ":" + accept.getPort() + "连接成功");
                StormThread stormThread = new StormThread(accept, mainFrame);
                stormThread.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
