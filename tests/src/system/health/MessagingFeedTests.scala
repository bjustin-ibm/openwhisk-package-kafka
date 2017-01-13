/*
 * Copyright 2015-2016 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package system.health

import java.util.HashMap
import java.util.Properties
import javax.security.auth.login.Configuration
import javax.security.auth.login.AppConfigurationEntry

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner

import common.JsHelpers
import common.TestHelpers
import common.TestUtils
import common.Wsk
import common.WskActorSystem
import common.WskProps
import common.WskTestHelpers
import spray.json.DefaultJsonProtocol.StringJsonFormat
import spray.json.JsArray
import spray.json.JsString
import spray.json.pimpAny

@RunWith(classOf[JUnitRunner])
class MessagingFeedTests
    extends FlatSpec
    with Matchers
    with WskActorSystem
    with BeforeAndAfterAll
    with TestHelpers
    with WskTestHelpers
    with JsHelpers {

    val topic = "test"
    val sessionTimeout = 10 seconds

    implicit val wskprops = WskProps()
    val wsk = new Wsk()

    val messagingPackage = "/whisk.system/messaging"
    val messageHubFeed = "messageHubFeed"

    def setMessageHubSecurityConfiguration(user: String, password: String) = {
        val map = new HashMap[String, String]()
        map.put("serviceName", "kafka")
        map.put("username", user)
        map.put("password", password)
        Configuration.setConfiguration(new Configuration()
        {
            def getAppConfigurationEntry(name: String): Array[AppConfigurationEntry] = Array(
    	          new AppConfigurationEntry (
    	              "com.ibm.messagehub.login.MessageHubLoginModule",
     			          AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, map))
        })
    }

    behavior of "Message Hub"

    it should "fire a trigger when a message is posted to the message hub" in withAssetCleaner(wskprops) {
        var credentials = TestUtils.getCredentials("message_hub")
        val user = credentials.get("user").getAsString()
        val password = credentials.get("password").getAsString()
        val kafka_admin_url = credentials.get("kafka_admin_url").getAsString()
        val api_key = credentials.get("api_key").getAsString()
        val kafka_brokers_sasl_json_array = credentials.get("kafka_brokers_sasl").getAsJsonArray()

        var vec = Vector[JsString]()
        var servers = s""
        val iter = kafka_brokers_sasl_json_array.iterator();
        while(iter.hasNext()){
            val server = iter.next().getAsString()
            vec = vec :+ JsString(server)
            servers = s"$servers$server,"
        }
        var kafka_brokers_sasl = JsArray(vec)

        System.setProperty("java.security.auth.login.config", "")
        setMessageHubSecurityConfiguration(user, password)
        var props = new Properties()
        props.put("bootstrap.servers", servers);
        props.put("security.protocol", "SASL_SSL");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        (wp, assetHelper) =>
        var iteration = 0

        while(true) {
            iteration += 1
            val currentTime = s"${System.currentTimeMillis}"
            val triggerName = s"/_/dummyMessageHubTrigger-$currentTime"
            println(s"\nCreating a new trigger #${iteration}: ${triggerName}")
            val feedCreationResult = wsk.trigger.create(triggerName, feed = Some(s"$messagingPackage/$messageHubFeed"), parameters = Map(
                        "user" -> user.toJson,
                        "password" -> password.toJson,
                        "api_key" -> api_key.toJson,
                        "kafka_admin_url" -> kafka_admin_url.toJson,
                        "kafka_brokers_sasl" -> kafka_brokers_sasl,
                        "topic" -> topic.toJson))

            println("Waiting for trigger create")
            withActivation(wsk.activation, feedCreationResult, initialWait = 5 seconds, totalWait = 60 seconds) {
                activation =>
                    // should be successful
                    activation.response.success shouldBe true
            }

            // It takes a moment for the consumer to fully initialize. We choose 2 seconds
            // as a temporary length of time to wait for.
            println("Sleepy time...")
            Thread.sleep(5000)

            println("Creating producer")
            val producer = new KafkaProducer[String, String](props)
            val record = new ProducerRecord(topic, "key", currentTime)

            println("Posting message")
            producer.send(record)
            producer.close()

            println("Polling for activations")
            val activations = wsk.activation.pollFor(N = 1, Some(triggerName), retries = 30)
            var triggerFired = false
            assert(activations.length > 0)
            for (id <- activations) {
                println(s"Waiting for activation ${id}")
                val activation = wsk.activation.waitForActivation(id)
                if (activation.isRight) {
                    // Check if the trigger is fired with the specific message, which is the current time
                    // generated.
                    if (activation.right.get.fields.get("response").toString.contains(currentTime))
                        triggerFired = true
                }
            }
            assert(triggerFired == true)

            // explicitly delete the trigger
            if((iteration % 5) != 0) {
                println("Deleting trigger")
                val feedDeletionResult = wsk.trigger.delete(triggerName)
                feedDeletionResult.stdout should include("ok")
            } else {
                println("I think I'll keep this trigger...")
            }
        }
    }
}
