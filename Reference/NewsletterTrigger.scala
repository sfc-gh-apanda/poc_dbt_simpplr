package com.simpplr.shared.trigger

import com.simpplr.shared.Newsletter
import com.snowflake.snowpark.Session

object NewsletterTrigger {

  def run(session: Session): String ={
    val newsletter = new Newsletter(session, "Newsletter", "shared_services_staging.vw_enl_newsletter", "udl.newsletter")
    newsletter.transform().message
  }
}
