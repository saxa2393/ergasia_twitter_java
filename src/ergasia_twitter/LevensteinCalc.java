/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ergasia_twitter;

/**
 *
 * @author alexandros
 */

public class LevensteinCalc{

        /**
         * Calculates the distance between the identifiers.
         * We ignore the case of the letters in the comparison.
         * @return between 0 (identical) and 1 (completely different)
         */
        public Number calculateDistance(String s1, String s2) {
                Double result = 1.0;

                if (s1 != null && s2 != null) {
                        result = calculateNormalizedDistance(s1, s2);
                }
                return result;
        }

        /**
         * Calculates the normalized Levenshtein distance.
         * We ignore the case of the letters in the comparison.
         * @return between 0 (identical) and 1 (completely different)
         */
        protected Double calculateNormalizedDistance(String s1, String s2) {
                Double lDistance = 0.0;
                
                s1 = s1.toLowerCase();
                s2 = s2.toLowerCase();
                int distance = Levenstein2.distance(s1, s2);
                lDistance = distance / (double)(s1.length() + s2.length());
                
                return lDistance;
        }

        

}
