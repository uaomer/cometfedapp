
package tassl.automate.federation;

import java.util.Random;

/**
 *
 * @author Javier, Mengsong
 */
public class GeneratePoisson {

    public static double nextTime(float rateParameter, Random gen) {
        return -Math.log((float) 1.0 - gen.nextFloat()) / rateParameter;
    }

    public static void main(String[] args) {
        Random generator = new Random();
        //200 tasks with a probability of 40 task per second.
        String taskType = "energyplus";
        double deadline = 25;
        int endInterval = 1000;
        long currentTime = 0;
        long value;
        int tasktypeVal = 0;
        //System.out.println("CurrentTime:NextEvent:TaskType:Deadline");
        System.out.println(currentTime + ":" + 0 + ":" + taskType + ":" + deadline);
        while (currentTime < endInterval) {
            value = Math.round(nextTime((float) 2 / 3, generator))*5;

            currentTime += value;            
            taskType = "energyplus";
            deadline = 25;
            /*
            tasktypeVal = generator.nextInt(3);
            if (tasktypeVal == 0) {
                taskType = "multiscale";
                deadline = 65000;
            } else if (tasktypeVal == 1) {
                taskType = "histogram";
                deadline = 30000;
            } else if (tasktypeVal == 2) {
                taskType = "texture";
                deadline = 75000;
            }
            */  
            System.out.println(currentTime + ":" + value + ":" + taskType + ":" + deadline);
        }
    }
}
