package neu.sw.hxs.ui;

import javax.swing.*;
import java.awt.*;
import java.util.HashMap;

public class DrawPanel extends JPanel {

    private Color[] colors = {Color.GREEN, Color.orange, Color.red, Color.yellow};
    private HashMap<String, Integer> infoMap = new HashMap<>();

    @Override
    protected void paintComponent(Graphics g) {
        super.paintComponent(g);
        int panelWidth = getWidth();
        int panelHeight = getHeight();
        Graphics2D g2 = (Graphics2D) g;
        HashMap<String, Integer> tempMap = new HashMap<>();
        tempMap.putAll(infoMap);
        int count = tempMap.size() * 3 + 1;
        int width = panelWidth / count;
        int max = getMax(tempMap);
        double rate = (panelHeight * 0.9) / max;
        int index = 0;
        g2.drawLine(0, 30, panelWidth, 30);
        for (String key : infoMap.keySet()) {
            g2.drawString(key, (3 * index + 1) * width, 20);
            g2.setColor(colors[(index + 1) % 4]);
            g2.fillRect((3 * index + 1) * width, 31, width * 2, (int) (infoMap.get(key) * rate));
            g2.setColor(Color.black);
            g2.drawString(infoMap.get(key).toString(), (3 * index + 2) * width, (int) (infoMap.get(key) * rate + 40));
            index++;
        }
    }

    public void set(String key, Integer value) {
        infoMap.put(key, value);
        repaint();
    }

    private int getMax(HashMap<String, Integer> hashMap) {
        int max = 0;
        for (String key : hashMap.keySet()) {
            if (hashMap.get(key) > max)
                max = hashMap.get(key);
        }
        return max;
    }
}
