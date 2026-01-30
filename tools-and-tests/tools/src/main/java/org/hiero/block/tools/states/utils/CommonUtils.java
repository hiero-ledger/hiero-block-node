/*
 * (c) 2016-2019 Swirlds, Inc.
 *
 * This software is the confidential and proprietary information of
 * Swirlds, Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Swirlds.
 *
 * SWIRLDS MAKES NO REPRESENTATIONS OR WARRANTIES ABOUT THE SUITABILITY OF
 * THE SOFTWARE, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
 * TO THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE, OR NON-INFRINGEMENT. SWIRLDS SHALL NOT BE LIABLE FOR
 * ANY DAMAGES SUFFERED BY LICENSEE AS A RESULT OF USING, MODIFYING OR
 * DISTRIBUTING THIS SOFTWARE OR ITS DERIVATIVES.
 */

package org.hiero.block.tools.states.utils;


import java.awt.Dialog;
import java.awt.GraphicsEnvironment;
import java.awt.Window;
import java.awt.event.HierarchyEvent;
import java.awt.event.HierarchyListener;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import javax.sound.midi.MidiChannel;
import javax.sound.midi.MidiSystem;
import javax.sound.midi.Synthesizer;
import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.Clip;
import javax.swing.JOptionPane;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;

public class CommonUtils {
	/** the default charset used by swirlds */
	private static Charset defaultCharset = StandardCharsets.UTF_8;

	/** used by beep() */
	private static Synthesizer synthesizer;

	/** used by click(). It is opened and never closed. */
	private static Clip clip = null;
	/** used by click() */
	private static byte[] data = null;
	/** used by click() */
	private static AudioFormat format = null;

	/**
	 * Normalizes the string in accordance with the Swirlds default normalization method (NFD) and returns
	 * the bytes of that normalized String encoded in the Swirlds default charset (UTF8). This is important
	 * for having a consistent method of converting Strings to bytes that will guarantee that two identical
	 * strings will have an identical byte representation
	 *
	 * @param s
	 * 		the String to be converted to bytes
	 * @return a byte representation of the String
	 */
	public static byte[] getNormalisedStringBytes(String s) {
		if (s == null) {
			return new byte[0];
		}
		return Normalizer.normalize(s, Normalizer.Form.NFD).getBytes(defaultCharset);
	}

	/**
	 * Reverse of {@link #getNormalisedStringBytes(String)}
	 *
	 * @param bytes
	 * 		the bytes to convert
	 * @return a String created from the input bytes
	 */
	public static String getNormalisedStringFromBytes(byte[] bytes) {
		return new String(bytes, defaultCharset);
	}

	/**
	 * Play a beep sound. It is middle C, half volume, 20 milliseconds.
	 */
	public static void beep() {
		beep(60, 64, 20);
	}

	/**
	 * Make a beep sound.
	 *
	 * @param pitch
	 * 		the pitch, from 0 to 127, where 60 is middle C, 61 is C#, etc.
	 * @param velocity
	 * 		the "velocity" (volume, or speed with which the note is played). 0 is silent, 127 is max.
	 * @param duration
	 * 		the number of milliseconds the sound will play
	 */
	public static void beep(int pitch, int velocity, int duration) {
		try {
			if (synthesizer == null) {
				synthesizer = MidiSystem.getSynthesizer();
				synthesizer.open();
			}

			MidiChannel[] channels = synthesizer.getChannels();

			channels[0].noteOn(pitch, velocity);
			Thread.sleep(duration);
			channels[0].noteOff(60);
		} catch (Exception e) {
		}
	}

	/**
	 * Make a click sound.
	 */
	public static void click() {
		try {
			if (data == null) {
				data = new byte[] { 0, 127 };
				format = new AudioFormat(AudioFormat.Encoding.PCM_SIGNED,
						44100.0f, 16, 1, 2, 44100.0f, false);
				clip = AudioSystem.getClip();
				clip.open(format, data, 0, data.length);
			}
			clip.start(); // play the waveform in data
			while (clip.getFramePosition() < clip.getFrameLength()) {
				Thread.yield(); // busy wait, but it's only for a short time, and at least it yields
			}
			clip.stop(); // it should have already stopped
			clip.setFramePosition(0); // for next time, start over
		} catch (Exception e) {
		}
	}

	/**
	 * Compare two byte arrays. A longer array is greater than a shorter one. An empty array is greater
	 * than a null. If two arrays are the same length, then they are compared by the first element where they
	 * differ.
	 *
	 * @param x
	 * 		the first byte array to compare
	 * @param y
	 * 		the second byte array to compare
	 * @return the value {@code 0} if {@code x == y};
	 * 		the value {@code -1} if {@code x < y}; and
	 * 		the value {@code 1} if {@code x > y}
	 */
	static int compare(final byte[] x, final byte[] y) {
		if ((x == null && y == null) || x == y) {
			return 0;
		} else if (x != null && y == null) {
			return 1;
		} else if (x == null && y != null) {
			return -1;
		}

		int result = Integer.compare(x.length, y.length);

		if (result != 0) {
			return result;
		}

		for (int i = 0; i < x.length; i++) {
			result = Byte.compare(x[i], y[i]);

			if (result != 0) {
				return result;
			}
		}

		return 0;
	}

	/**
	 * Quickly return an object whose toString() calculates and returns the string representation of the
	 * given exception. If Settings.logStack is true, then it includes the exception name and a full stack
	 * trace with newlines after each level on the stack. If it is false, then only the name is included.
	 * <p>
	 * This Object can be passed to Log4J2 logging, and the work of creating the complicated string will
	 * only happen when the appropriate logging marker is active.
	 *
	 * @param e
	 * 		the exception
	 * @return a parameterless lambda which, when evaluated, returns the string.
	 */
	public static Object err(Throwable e) {
		Object obj = new Object() {
			@Override
			public String toString() {
				Throwable cause = e.getCause();
				String localizedMessage = e.getLocalizedMessage();
				String message = e.getMessage();
				StackTraceElement[] stackTrace = e.getStackTrace();
				Throwable[] suppressed = e.getSuppressed();
				String className = e.getClass().getName();

				String result = className + ": " + message;
				if (message != null && !message.equals(localizedMessage)) {
					result += "(" + localizedMessage + ") ";
				}
				if (cause != null) {
					result += "cause: [\n" + err(cause) + "\n]";
				}
				if (suppressed != null && suppressed.length > 0) {
					result += "suppressed: ";
					for (Throwable supp : suppressed) {
						result += "[\n" + err(supp) + "\n]";
					}
				}
				if (stackTrace != null ) {
					for (StackTraceElement elm : stackTrace) {
						result += "\n|   " + elm;
					}
				}
				return result;
			}
		};
		return obj;
	}

	/**
	 * This is equivalent to System.out.println(), but is not used for debugging; it is used for production
	 * code for communicating to the user. Centralizing it here makes it easier to search for debug prints
	 * that might have slipped through before a release.
	 *
	 * @param msg
	 * 		the message for the user
	 */
	public static void tellUserConsole(String msg) {
		System.out.println(msg);
	}

	/**
	 * This is equivalent to sending text to doing both Utilities.tellUserConsole() and writing to a popup
	 * window. It is not used for debugging; it is used for production code for communicating to the user.
	 *
	 * @param title
	 * 		the title of the window to pop up
	 * @param msg
	 * 		the message for the user
	 */
	public static void tellUserConsolePopup(String title, String msg) {
		tellUserConsole("\n***** " + msg + " *****\n");
		if (!GraphicsEnvironment.isHeadless()) {
			String[] ss = msg.split("\n");
			int w = 0;
			for (String str : ss) {
				w = Math.max(w, str.length());
			}
			JTextArea ta = new JTextArea(ss.length + 1, (int) (w * 0.65));
			ta.setText(msg);
			ta.setWrapStyleWord(true);
			ta.setLineWrap(true);
			ta.setCaretPosition(0);
			ta.setEditable(false);
			ta.addHierarchyListener(new HierarchyListener() { // make ta resizable
				public void hierarchyChanged(HierarchyEvent e) {
					Window window = SwingUtilities.getWindowAncestor(ta);
					if (window instanceof Dialog) {
						Dialog dialog = (Dialog) window;
						if (!dialog.isResizable()) {
							dialog.setResizable(true);
						}
					}
				}
			});
			JScrollPane sp = new JScrollPane(ta);
			JOptionPane.showMessageDialog(null, sp, title,
					JOptionPane.PLAIN_MESSAGE);
		}
	}

	/**
	 * Converts a single byte to a lowercase hexadecimal string.
	 *
	 * @param b
	 * 		the byte to convert to hexadecimal
	 * @return a {@link String} containing the lowercase hexadecimal representation of the byte
	 */
	public static String hex(byte b) {
		return String.format("%02x", b);
	}

	/**
	 * Converts an array of bytes to a lowercase hexadecimal string.
	 *
	 * @param bytes
	 * 		the array of bytes to hexadecimal
	 * @return a {@link String} containing the lowercase hexadecimal representation of the byte array
	 */
	public static String hex(byte[] bytes) {
		StringBuilder sb = new StringBuilder();

		if (bytes != null) {
			for (byte b : bytes) {
				sb.append(String.format("%02x", b));
			}
		}

		return sb.toString();
	}

	/**
	 * Converts a hexadecimal string back to the original array of bytes.
	 *
	 * @param bytes
	 * 		the hexadecimal string to be converted
	 * @return an array of bytes
	 */
	public static byte[] unhex(CharSequence bytes) {

		if (bytes.length() % 2 != 0) {
			throw new IllegalArgumentException("bytes");
		}

		final int len = bytes.length();
		final byte[] data = new byte[(len / 2)];
		for (int i = 0; i < len; i += 2) {
			data[(i / 2)] = (byte) ((Character.digit(bytes.charAt(i), 16) << 4)
					+ Character.digit(bytes.charAt(i + 1), 16));
		}

		return data;
	}
}
