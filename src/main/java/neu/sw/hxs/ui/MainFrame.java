package neu.sw.hxs.ui;

import javax.swing.*;
import java.awt.*;

public class MainFrame extends JFrame {
    private DrawPanel drawPanel;

    MainFrame() {
        drawPanel = new DrawPanel();
        setLayout(new BorderLayout());
        add(drawPanel, BorderLayout.CENTER);
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
        setSize(new Dimension(600, 800));
        setPreferredSize(new Dimension(600, 800));
    }

    public void put(String key, Integer value) {
        drawPanel.set(key, value);
        drawPanel.updateUI();
    }
}
