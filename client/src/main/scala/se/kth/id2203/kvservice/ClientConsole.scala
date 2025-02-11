/*
 * The MIT License
 *
 * Copyright 2017 Lars Kroll <lkroll@kth.se>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package se.kth.id2203.kvservice

import com.larskroll.common.repl._
import com.typesafe.scalalogging.StrictLogging;
import org.apache.log4j.Layout
import util.log4j.ColoredPatternLayout;
import fastparse._, NoWhitespace._
import concurrent.Await
import concurrent.duration._

object ClientConsole {
  def lowercase[_: P]: P[Unit] = P(CharIn("a-z"))
  def uppercase[_: P]: P[Unit] = P(CharIn("A-Z"))
  def digit[_: P]: P[Unit] = P(CharIn("0-9"))
  def simpleStr[_: P]: P[Unit] = P(lowercase | uppercase | digit)
  val colouredLayout = new ColoredPatternLayout("%d{[HH:mm:ss,SSS]} %-5p {%c{1}} %m%n");
}

case class PutObject(key: String, value: String)
case class CasObject(key: String, refValue: String, newValue: String)

class ClientConsole(val service: ClientService) extends CommandConsole with ParsedCommands with StrictLogging {
  import ClientConsole._;

  override def layout: Layout = colouredLayout;
  override def onInterrupt(): Unit = exit();

  val getParser: ParsingObject[String] = new ParsingObject[String] {
    override def parseOperation[_: P]: P[String] = P("get" ~ " " ~ simpleStr.rep.!);
  }

  val putParser: ParsingObject[PutObject] = new ParsingObject[PutObject] {
    override def parseOperation[_: P]: P[PutObject] =
      P("put" ~ " " ~ simpleStr.rep.! ~ " " ~ simpleStr.rep.!).map(x => PutObject(x._1, x._2))
  }

  val casParser: ParsingObject[CasObject] = new ParsingObject[CasObject] {
    override def parseOperation[_: P]: P[CasObject] =
      P("cas" ~ " " ~ simpleStr.rep.! ~ " " ~ simpleStr.rep.! ~ " " ~ simpleStr.rep.!)
        .map(x => CasObject(x._1, x._2, x._3))
  }

  val putCommand: ParsedCommand[PutObject] = parsed(putParser, usage = "put <key> <value>", descr = "Executes put for <key> <value>.") {
    case PutObject(key, value) =>
      val fr = service.put(key, value);
      out.println("Operation PUT sent! Awaiting response...");
      try {
        val r = Await.result(fr, 5.seconds);
        out.println("Operation PUT complete! Response was: " + r.status);
      } catch {
        case e: Throwable => logger.error("Error during PUT op.", e);
      }
  }

  val getCommand: ParsedCommand[String] = parsed(getParser, usage = "get <key>", descr = "Executes get for <key>.") { key =>
    val fr = service.get(key);
    out.println("Operation GET sent! Awaiting response...");
    try {
      val r = Await.result(fr, 5.seconds);
      out.println("Operation GET complete! Response was: " + r.status + " and value: " + r.value);
    } catch {
      case e: Throwable => logger.error("Error during GET op.", e);
    }
  }
  val casCommand: ParsedCommand[CasObject] = parsed(casParser,
                          usage = "cas <key> <ref-value> <new-value>",
                          descr = "Executes cas for <key> <ref-value> <new-value>.") {
    case CasObject(key, refValue, newValue) =>
      val fr = service.cas(key, refValue, newValue);
      out.println("Operation CAS sent! Awaiting response...");
      try {
        val r = Await.result(fr, 5.seconds);
        out.println("Operation CAS complete! Response was: " + r.status + " and value: " + r.value);
      } catch {
        case e: Throwable => logger.error("Error during CAS op.", e);
      }
  }

}
