package utils;

import java.util.regex.Pattern;

public class Utils {

    /**
     * Returns if a string has a character of the Chinese-Japanese-Korean characters.
     *
     * @param str the string to be tested
     * @return true if CJK, false otherwise
     */
    public static boolean isCJK(String str) {

        for (char c : str.toCharArray()) {


            if ((Character.UnicodeBlock.of(c) == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS)
                    || (Character.UnicodeBlock.of(c) == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A)
                    || (Character.UnicodeBlock.of(c) == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B)
                    || (Character.UnicodeBlock.of(c) == Character.UnicodeBlock.CJK_COMPATIBILITY_FORMS)
                    || (Character.UnicodeBlock.of(c) == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS)
                    || (Character.UnicodeBlock.of(c) == Character.UnicodeBlock.CJK_RADICALS_SUPPLEMENT)
                    || (Character.UnicodeBlock.of(c) == Character.UnicodeBlock.CJK_SYMBOLS_AND_PUNCTUATION)
                    || (Character.UnicodeBlock.of(c) == Character.UnicodeBlock.ENCLOSED_CJK_LETTERS_AND_MONTHS)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if string contains any cyrillic word
     *
     * @param str string to be tested
     * @return true if Cyrillic, false otherwise
     */
    public static boolean isCyrillic(String str) {
        return Pattern.matches(".*\\p{InCyrillic}.*", str);
    }
}
