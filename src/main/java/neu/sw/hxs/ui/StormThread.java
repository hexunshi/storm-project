package neu.sw.hxs.ui;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StormThread extends Thread {
    private Socket socket;
    private MainFrame mainFrame;

    public StormThread(Socket socket, MainFrame mainFrame) {
        this.socket = socket;
        this.mainFrame = mainFrame;
    }

    @Override
    public void run() {
        DataInputStream dataInputStream = null;
        try {
            dataInputStream = new DataInputStream(socket.getInputStream());
            while (true) {
                String s = dataInputStream.readUTF();
                System.out.println(s);
                Pattern compile = Pattern.compile("\\{(.*?),(.*?)\\}");
                Matcher matcher = compile.matcher(s);
                if (matcher.find()) {
                    if (matcher.groupCount() == 2) {
                        mainFrame.put(matcher.group(1), Integer.parseInt(matcher.group(2)));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
