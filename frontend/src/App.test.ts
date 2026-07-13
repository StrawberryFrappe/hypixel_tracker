import { mount } from "@vue/test-utils";
import { describe, expect, it } from "vitest";

import App from "./App.vue";

describe("App", () => {
  it("labels scaffold data honestly", () => {
    const wrapper = mount(App);

    expect(wrapper.get("h1").text()).toContain("awaiting signal");
    expect(wrapper.get('[role="status"]').text()).toContain("NO LIVE DATA");
    expect(wrapper.text()).toContain("No fabricated market rows");
    expect(wrapper.text()).toContain("ADVISORY ONLY");
  });
});
