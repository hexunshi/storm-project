package neu.sw.hxs.ui;

import java.io.IOException;

public class Domain {
    public static void main(String[] args) throws IOException {
        MainFrame mainFrame = new MainFrame();
        mainFrame.setVisible(true);
        ListenerThread listenerThread = new ListenerThread(mainFrame, 5555);
        listenerThread.start();
    }
}
