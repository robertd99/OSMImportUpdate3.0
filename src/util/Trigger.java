package util;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author thsc
 */
public class Trigger extends Thread {
    private final TriggerRecipient rec;
    private boolean stopped;
    private long milliseconds;
    
    public Trigger(TriggerRecipient rec, long milliseconds) {
        this.rec = rec;
        this.stopped = false;
        this.milliseconds = milliseconds;
    }
    
    public void end() {
        this.stopped = true;
    }
    
    public void setMilliseconds(long milliseconds) {
        this.milliseconds = milliseconds;
    }
    
    @Override
    public void run() {
        while(!this.stopped) {
            try {
                Thread.sleep(this.milliseconds);
                if(!stopped) {
                    this.rec.trigger();
                }
            } catch (InterruptedException ex) {
                //
            }
        }
    }
}
