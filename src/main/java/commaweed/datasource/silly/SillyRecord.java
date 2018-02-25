package commaweed.datasource.silly;

import java.io.Serializable;

public class SillyRecord implements Serializable {

   private String type;
   private int count;
   private double amount;

   public String getType() {
      return type;
   }

   public void setType(String type) {
      this.type = type;
   }

   public int getCount() {
      return count;
   }

   public void setCount(int count) {
      this.count = count;
   }

   public double getAmount() {
      return amount;
   }

   public void setAmount(double amount) {
      this.amount = amount;
   }
}
